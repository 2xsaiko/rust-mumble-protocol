//! Control channel messages and codecs

use std::io;
use std::io::Cursor;
use std::marker::PhantomData;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use protobuf::error::ProtobufError;
use protobuf::Message;

use crate::voice::Clientbound;
use crate::voice::Serverbound;
use crate::voice::VoiceCodec;
use crate::voice::VoicePacket;
use crate::voice::VoicePacketDst;

/// ProtoBuf message types for all Mumble messages.
#[allow(renamed_and_removed_lints)] // protobuf is missing `clippy::` prefix
#[allow(missing_docs)] // these would have to be auto-generated by protobuf
pub mod msgs {
    /// Mumble message type to packet ID mappings.
    pub mod id {
        pub use super::super::generated_id::*;
    }

    include!(concat!(env!("OUT_DIR"), "/proto/mod.rs"));
}

/// Raw/not-yet-parsed Mumble control packet.
#[derive(Clone, Debug, PartialEq)]
pub struct RawControlPacket {
    /// Packet ID
    ///
    /// See [msgs::id].
    pub id: u16,
    /// Raw message bytes.
    pub bytes: Bytes,
}

/// A `Codec` implementation that parses a stream of data into [RawControlPacket]s.
#[derive(Debug)]
pub struct RawControlCodec;

impl RawControlCodec {
    /// Creates a new RawControlCodec.
    pub fn new() -> Self {
        Default::default()
    }
}

impl Default for RawControlCodec {
    fn default() -> Self {
        RawControlCodec
    }
}

impl RawControlCodec {
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<RawControlPacket>, io::Error> {
        let buf_len = buf.len();
        if buf_len >= 6 {
            let mut buf = Cursor::new(buf);
            let id = buf.get_u16();
            let len = buf.get_u32() as usize;
            if len > 0x7f_ffff {
                Err(io::Error::new(io::ErrorKind::Other, "packet too long"))
            } else if buf_len >= 6 + len {
                let mut bytes = buf.into_inner().split_to(6 + len);
                bytes.advance(6);
                let bytes = bytes.freeze();
                Ok(Some(RawControlPacket { id, bytes }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "tokio-codec")]
impl tokio_util::codec::Decoder for RawControlCodec {
    type Item = RawControlPacket;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src)
    }
}

#[cfg(feature = "asynchronous-codec")]
impl asynchronous_codec::Decoder for RawControlCodec {
    type Item = RawControlPacket;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src)
    }
}

impl RawControlCodec {
    fn encode(&mut self, item: RawControlPacket, dst: &mut BytesMut) -> Result<(), io::Error> {
        let id = item.id;
        let bytes = &item.bytes;
        let len = bytes.len();
        dst.reserve(6 + len);
        dst.put_u16(id);
        dst.put_u32(len as u32);
        dst.put_slice(bytes);
        Ok(())
    }
}

#[cfg(feature = "tokio-codec")]
impl tokio_util::codec::Encoder<RawControlPacket> for RawControlCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RawControlPacket, dst: &mut BytesMut) -> Result<(), io::Error> {
        self.encode(item, dst)
    }
}

#[cfg(feature = "asynchronous-codec")]
impl asynchronous_codec::Encoder for RawControlCodec {
    type Item = RawControlPacket;
    type Error = io::Error;

    fn encode(&mut self, item: RawControlPacket, dst: &mut BytesMut) -> Result<(), io::Error> {
        self.encode(item, dst)
    }
}

/// A `Codec` implementation that parses a stream of data into [ControlPacket]s.
///
/// Since [VoicePacket]s can be tunneled over the control channel and their encoding and decoding
/// depends on their destination, the control codec also needs to know the side it's on.
/// See [ServerControlCodec] and [ClientControlCodec] for the two most reasonable configurations.
#[derive(Debug)]
pub struct ControlCodec<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> {
    inner: RawControlCodec,
    _encode_dst: PhantomData<EncodeDst>,
    _decode_dst: PhantomData<DecodeDst>,
}
/// The [ControlCodec] used on the server side.
pub type ServerControlCodec = ControlCodec<Clientbound, Serverbound>;
/// The [ControlCodec] used on the client side.
pub type ClientControlCodec = ControlCodec<Serverbound, Clientbound>;

impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> ControlCodec<EncodeDst, DecodeDst> {
    /// Creates a new control codec.
    pub fn new() -> Self {
        Default::default()
    }
}

impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> Default
    for ControlCodec<EncodeDst, DecodeDst>
{
    fn default() -> Self {
        ControlCodec {
            inner: RawControlCodec::default(),
            _encode_dst: PhantomData,
            _decode_dst: PhantomData,
        }
    }
}

impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> ControlCodec<EncodeDst, DecodeDst> {
    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<ControlPacket<DecodeDst>>, io::Error> {
        Ok(if let Some(raw_packet) = self.inner.decode(src)? {
            Some(raw_packet.try_into()?)
        } else {
            None
        })
    }
}

#[cfg(feature = "tokio-codec")]
impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> tokio_util::codec::Decoder
    for ControlCodec<EncodeDst, DecodeDst>
{
    type Item = ControlPacket<DecodeDst>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src)
    }
}

#[cfg(feature = "asynchronous-codec")]
impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> asynchronous_codec::Decoder
    for ControlCodec<EncodeDst, DecodeDst>
{
    type Item = ControlPacket<DecodeDst>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src)
    }
}

#[cfg(feature = "tokio-codec")]
impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst>
    tokio_util::codec::Encoder<ControlPacket<EncodeDst>> for ControlCodec<EncodeDst, DecodeDst>
{
    type Error = io::Error;

    fn encode(
        &mut self,
        item: ControlPacket<EncodeDst>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item.into(), dst)
    }
}

#[cfg(feature = "asynchronous-codec")]
impl<EncodeDst: VoicePacketDst, DecodeDst: VoicePacketDst> asynchronous_codec::Encoder
    for ControlCodec<EncodeDst, DecodeDst>
{
    type Item = ControlPacket<EncodeDst>;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item.into(), dst)
    }
}

/// Generates packet to ID mappings which will end up in [msgs::ids].
macro_rules! define_packet_mappings {
    ( @def $id:expr, $name:ident) => {
        #[allow(dead_code)]
        #[allow(non_upper_case_globals)]
        pub const $name: u16 = $id;
    };
    ( @rec $id:expr, $(#[$attr:meta])* $head:ident ) => {
        $(#[$attr])*
        define_packet_mappings!(@def $id, $head);
    };
    ( @rec $id:expr, $(#[$attr:meta])* $head:ident, $( $(#[$attr_tail:meta])* $tail:ident ),* ) => {
        $(#[$attr])*
        define_packet_mappings!(@def $id, $head);
        define_packet_mappings!(@rec $id + 1, $($(#[$attr_tail])* $tail),*);
    };
    ( $( $(#[$attrs:meta])* $names:ident ),* ) => {
        define_packet_mappings!(@rec 0, $($(#[$attrs])* $names),*);
    };
}

/// Generates From impls for converting between RawCtrlPck <=> ProtoMsg => CtrlPck
macro_rules! define_packet_from {
    ( $Dst:ident UDPTunnel($type:ty) ) => {
        impl<$Dst: VoicePacketDst> From<VoicePacket<Dst>> for RawControlPacket {
            fn from(msg: VoicePacket<Dst>) -> Self {
                let mut buf = BytesMut::new();

                cfg_if::cfg_if! {
                    if #[cfg(feature = "asynchronous-codec")] {
                        use asynchronous_codec::Encoder as _;
                    } else {
                        use tokio_util::codec::Encoder as _;
                    }
                }

                VoiceCodec::<Dst, Dst>::default()
                    .encode(msg, &mut buf)
                    .expect("VoiceEncoder is infallible");

                Self {
                    id: msgs::id::UDPTunnel,
                    bytes: buf.freeze(),
                }
            }
        }
        impl<$Dst: VoicePacketDst> TryFrom<RawControlPacket> for VoicePacket<$Dst> {
            type Error = io::Error;

            fn try_from(packet: RawControlPacket) -> Result<Self, Self::Error> {
                if packet.id == msgs::id::UDPTunnel {
                    packet.bytes.try_into()
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        concat!("expected packet of type ", stringify!(UDPTunnel)),
                    ))
                }
            }
        }
        impl<$Dst: VoicePacketDst> TryFrom<Bytes> for VoicePacket<$Dst> {
            type Error = io::Error;

            fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "asynchronous-codec")] {
                        use asynchronous_codec::Decoder as _;
                    } else {
                        use tokio_util::codec::Decoder as _;
                    }
                }

                VoiceCodec::<$Dst, $Dst>::default()
                    .decode(&mut BytesMut::from(bytes.as_ref()))
                    .map(|it| it.expect("VoiceCodec is stateless"))
            }
        }
        impl<$Dst: VoicePacketDst> From<$type> for ControlPacket<$Dst> {
            fn from(inner: $type) -> Self {
                ControlPacket::UDPTunnel(Box::new(inner))
            }
        }
    };
    ( $Dst:ident $name:ident($type:ty) ) => {
        impl<$Dst: VoicePacketDst> From<$type> for ControlPacket<$Dst> {
            fn from(inner: $type) -> Self {
                ControlPacket::$name(Box::new(inner))
            }
        }
        impl From<$type> for RawControlPacket {
            fn from(msg: $type) -> Self {
                Self {
                    id: self::msgs::id::$name,
                    bytes: msg.write_to_bytes().unwrap().into(),
                }
            }
        }
        impl TryFrom<RawControlPacket> for $type {
            type Error = ProtobufError;

            fn try_from(packet: RawControlPacket) -> Result<Self, Self::Error> {
                if packet.id == msgs::id::$name {
                    Self::try_from(packet.bytes)
                } else {
                    Err(ProtobufError::IoError(io::Error::new(
                        io::ErrorKind::Other,
                        concat!("expected packet of type ", stringify!($name)),
                    )))
                }
            }
        }
        impl TryFrom<&[u8]> for $type {
            type Error = ProtobufError;

            fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
                Message::parse_from_bytes(bytes)
            }
        }
        impl TryFrom<Bytes> for $type {
            type Error = ProtobufError;

            fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
                bytes.as_ref().try_into()
            }
        }
    };
}

/// Generates the ControlPacket enum, From impls for RawCtrlPck <=> CtrlPck and CtrlPck::name()
macro_rules! define_packet_enum {
    ( $Dst:ident $( $(#[$attr:meta])* $name:ident($type:ty) ),* ) => {
        /// A parsed Mumble control packet.
        #[derive(Debug, Clone, PartialEq)]
        #[allow(clippy::large_enum_variant)]
        #[non_exhaustive]
        pub enum ControlPacket<$Dst: VoicePacketDst> {
            $(
                #[allow(missing_docs)]
                $(#[$attr])*
                $name(Box<$type>),
            )*
            /// A packet of unknown type.
            Other(RawControlPacket),
        }
        impl<Dst: VoicePacketDst> TryFrom<RawControlPacket> for ControlPacket<$Dst> {
            type Error = ProtobufError;

            fn try_from(packet: RawControlPacket) -> Result<Self, Self::Error> {
                Ok(match packet.id {
                    $(
                        $(#[$attr])*
                        msgs::id::$name => {
                            ControlPacket::$name(Box::new(packet.bytes.try_into()?))
                        }
                    )*
                        _ => ControlPacket::Other(packet),
                })
            }
        }
        impl<Dst: VoicePacketDst> From<ControlPacket<$Dst>> for RawControlPacket {
            fn from(packet: ControlPacket<$Dst>) -> Self {
                match packet {
                    $(
                        $(#[$attr])*
                        ControlPacket::$name(inner) => (*inner).into(),
                    )*
                        ControlPacket::Other(inner) => inner,
                }
            }
        }
        impl<Dst: VoicePacketDst> ControlPacket<$Dst> {
            /// Returns the internal name of a packet (for debugging purposes).
            pub fn name(&self) -> &'static str {
                match self {
                    $(
                        $(#[$attr])*
                        ControlPacket::$name(_) => stringify!($name),
                    )*
                    ControlPacket::Other(_) => "unknown",
                }
            }
        }
    };
}

macro_rules! define_packets {
    ( < $Dst:ident > $( $(#[$attr:meta])* $name:ident($type:ty), )* ) => {
        #[allow(missing_docs)]
        mod generated_id {
            define_packet_mappings!($($(#[$attr])* $name),*);
        }
        define_packet_enum!($Dst $($(#[$attr])* $name($type)),*);
        $(
            $(#[$attr])*
            define_packet_from!($Dst $name($type));
        )*
    };
}

define_packets![
    <Dst>
    Version(msgs::Version),
    UDPTunnel(VoicePacket<Dst>),
    Authenticate(msgs::Authenticate),
    Ping(msgs::Ping),
    Reject(msgs::Reject),
    ServerSync(msgs::ServerSync),
    ChannelRemove(msgs::ChannelRemove),
    ChannelState(msgs::ChannelState),
    UserRemove(msgs::UserRemove),
    UserState(msgs::UserState),
    BanList(msgs::BanList),
    TextMessage(msgs::TextMessage),
    PermissionDenied(msgs::PermissionDenied),
    ACL(msgs::ACL),
    QueryUsers(msgs::QueryUsers),
    CryptSetup(msgs::CryptSetup),
    ContextActionModify(msgs::ContextActionModify),
    ContextAction(msgs::ContextAction),
    UserList(msgs::UserList),
    VoiceTarget(msgs::VoiceTarget),
    PermissionQuery(msgs::PermissionQuery),
    CodecVersion(msgs::CodecVersion),
    UserStats(msgs::UserStats),
    RequestBlob(msgs::RequestBlob),
    ServerConfig(msgs::ServerConfig),
    SuggestConfig(msgs::SuggestConfig),
    #[cfg(feature = "webrtc-extensions")]
    WebRTC(msgs::WebRTC),
    #[cfg(feature = "webrtc-extensions")]
    IceCandidate(msgs::IceCandidate),
    #[cfg(feature = "webrtc-extensions")]
    TalkingState(msgs::TalkingState),
];
