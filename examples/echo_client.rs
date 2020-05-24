use argparse::ArgumentParser;
use argparse::Store;
use argparse::StoreTrue;
use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::join;
use futures::StreamExt;
use futures::SinkExt;
use asynchronous_codec::{Decoder, Encoder};
use mumble_protocol::control::msgs;
use mumble_protocol::control::ClientControlCodec;
use mumble_protocol::control::ControlPacket;
use mumble_protocol::crypt::ClientCryptState;
use mumble_protocol::voice::VoicePacket;
use mumble_protocol::voice::VoicePacketPayload;
use std::convert::Into;
use std::convert::TryInto;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use async_native_tls::TlsConnector;
use async_std::net::{UdpSocket, TcpStream};
use asynchronous_codec::Framed;

async fn connect(
    server_addr: SocketAddr,
    server_host: String,
    user_name: String,
    accept_invalid_cert: bool,
    crypt_state_sender: oneshot::Sender<ClientCryptState>,
) {
    // Wrap crypt_state_sender in Option, so we can call it only once
    let mut crypt_state_sender = Some(crypt_state_sender);

    // Connect to server via TCP
    let stream = TcpStream::connect(&server_addr).await.expect("Failed to connect to server:");
    println!("TCP connected..");

    // Wrap the connection in TLS
    let connector = TlsConnector::new()
        .danger_accept_invalid_certs(accept_invalid_cert)
        .use_sni(true);
    let tls_stream = connector
        .connect(&server_host, stream)
        .await
        .expect("Failed to connect TLS: {}");
    println!("TLS connected..");

    // Wrap the TLS stream with Mumble's client-side control-channel codec
    let (mut sink, mut stream) = Framed::new(tls_stream, ClientControlCodec::new()).split();

    // Handshake (omitting `Version` message for brevity)
    let mut msg = msgs::Authenticate::new();
    msg.set_username(user_name);
    msg.set_opus(true);
    sink.send(msg.into()).await.unwrap();

    println!("Logging in..");
    let mut crypt_state = None;

    // Note: A normal application also has to send periodic Ping packets

    // Handle incoming packets
    while let Some(packet) = stream.next().await {
        match packet.unwrap() {
            ControlPacket::TextMessage(mut msg) => {
                println!(
                    "Got message from user with session ID {}: {}",
                    msg.get_actor(),
                    msg.get_message()
                );
                // Send reply back to server
                let mut response = msgs::TextMessage::new();
                response.mut_session().push(msg.get_actor());
                response.set_message(msg.take_message());
                sink.send(response.into()).await.unwrap();
            }
            ControlPacket::CryptSetup(msg) => {
                // Wait until we're fully connected before initiating UDP voice
                crypt_state = Some(ClientCryptState::new_from(
                    msg.get_key()
                        .try_into()
                        .expect("Server sent private key with incorrect size"),
                    msg.get_client_nonce()
                        .try_into()
                        .expect("Server sent client_nonce with incorrect size"),
                    msg.get_server_nonce()
                        .try_into()
                        .expect("Server sent server_nonce with incorrect size"),
                ));
            }
            ControlPacket::ServerSync(_) => {
                println!("Logged in!");
                if let Some(sender) = crypt_state_sender.take() {
                    let _ = sender.send(
                        crypt_state
                            .take()
                            .expect("Server didn't send us any CryptSetup packet!"),
                    );
                }
            }
            ControlPacket::Reject(msg) => {
                println!("Login rejected: {:?}", msg);
            }
            _ => {},
        }
    }
}

async fn handle_udp(
    server_addr: SocketAddr,
    crypt_state: oneshot::Receiver<ClientCryptState>,
) {
    let mut recv_buf = [0_u8; 1024];

    // Bind UDP socket
    let udp_socket = UdpSocket::bind((Ipv6Addr::from(0u128), 0u16))
        .await
        .expect("Failed to bind UDP socket");

    // Wait for initial CryptState
    let mut crypt_state = match crypt_state.await {
        Ok(crypt_state) => crypt_state,
        // disconnected before we received the CryptSetup packet, oh well
        Err(_) => return,
    };
    println!("UDP ready!");

    let mut buf = BytesMut::with_capacity(1024);
    crypt_state.encode(VoicePacket::Audio {
        _dst: std::marker::PhantomData,
        target: 0,
        session_id: (),
        seq_num: 0,
        payload: VoicePacketPayload::Opus(Bytes::from([0u8; 128].as_ref()), true),
        position_info: None,
    }, &mut buf).unwrap();

    // Note: A normal application would also send periodic Ping packets, and its own audio
    //       via UDP. We instead trick the server into accepting us by sending it one
    //       dummy voice packet.
    udp_socket.send_to(&buf.to_vec(), server_addr).await.unwrap();

    // Handle incoming UDP packets
    while let Ok((num_read, src_addr)) = udp_socket.recv_from(&mut recv_buf).await {
        let packet = crypt_state.decode(&mut BytesMut::from(&recv_buf[..num_read]));
        let packet = match packet {
            Ok(Some(packet)) => packet,
            Ok(None) => {
                eprintln!("Problem decoding packet");
                continue;
            },
            Err(err) => {
                eprintln!("Got an invalid UDP packet: {}", err);
                // To be expected, considering this is the internet, just ignore it
                continue
            }
        };
        match packet {
            VoicePacket::Ping { .. } => {
                // Note: A normal application would handle these and only use UDP for voice
                //       once it has received one.
                continue
            }
            VoicePacket::Audio {
                seq_num,
                payload,
                position_info,
                ..
            } => {
                // Got audio, naively echo it back
                let reply = VoicePacket::Audio {
                    _dst: std::marker::PhantomData,
                    target: 0,      // normal speech
                    session_id: (), // unused for server-bound packets
                    seq_num,
                    payload,
                    position_info,
                };
                crypt_state.encode(reply, &mut buf).unwrap();
                udp_socket.send_to(&buf.to_vec(), src_addr).await.unwrap();
            }
        }
    }
}

#[async_std::main]
async fn main() {
    // Handle command line arguments
    let mut server_host = "".to_string();
    let mut server_port = 64738u16;
    let mut user_name = "EchoBot".to_string();
    let mut accept_invalid_cert = false;
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Run the echo client example");
        ap.refer(&mut server_host)
            .add_option(&["--host"], Store, "Hostname of mumble server")
            .required();
        ap.refer(&mut server_port)
            .add_option(&["--port"], Store, "Port of mumble server");
        ap.refer(&mut user_name)
            .add_option(&["--username"], Store, "User name used to connect");
        ap.refer(&mut accept_invalid_cert).add_option(
            &["--accept-invalid-cert"],
            StoreTrue,
            "Accept invalid TLS certificates",
        );
        ap.parse_args_or_exit();
    }
    let server_addr = (server_host.as_ref(), server_port)
        .to_socket_addrs()
        .expect("Failed to parse server address")
        .next()
        .expect("Failed to resolve server address");

    // Oneshot channel for setting UDP CryptState from control task
    // For simplicity we don't deal with re-syncing, real applications would have to.
    let (crypt_state_sender, crypt_state_receiver) = oneshot::channel::<ClientCryptState>();

    // Run it
    join!(
        connect(
            server_addr,
            server_host,
            user_name,
            accept_invalid_cert,
            crypt_state_sender,
        ),
        handle_udp(server_addr, crypt_state_receiver)
    );
}
