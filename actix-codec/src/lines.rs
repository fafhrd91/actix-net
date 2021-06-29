use std::io;

use bytes::{Buf, BytesMut};
use memchr::memchr;

use super::{Decoder, Encoder};

/// Bytes codec.
///
/// Reads/Writes chunks of bytes from a stream.
#[derive(Debug, Copy, Clone)]
pub struct LinesCodec;

impl Encoder<String> for LinesCodec {
    type Error = io::Error;

    #[inline]
    fn encode(&mut self, item: String, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(item.as_ref());
        Ok(())
    }
}

impl Decoder for LinesCodec {
    type Item = String;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            Ok(None)
        } else {
            let n = match memchr(b'\n', src.chunk()) {
                Some(n) => n,
                None => {
                    return Ok(None);
                }
            };

            // split up to new line char
            let mut buf = src.split_to(n);
            let len = buf.len();

            // remove new line char from source
            src.advance(1);

            match buf.last() {
                Some(char) if *char == b'\r' => {
                    // remove one carriage returns at the end of buf
                    buf.truncate(len - 1);
                }
                None => return Ok(Some(String::new())),
                _ => {}
            }

            to_utf8(buf)
        }
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(src)? {
            Some(frame) => Ok(Some(frame)),
            None if src.is_empty() => Ok(None),
            None => {
                let len = src.len();
                let buf = if src[len - 1] == b'\r' {
                    src.split_to(len - 1)
                } else {
                    src.split()
                };

                if buf.is_empty() {
                    return Ok(None);
                }

                return to_utf8(buf);
            }
        }
    }
}

fn to_utf8(buf: BytesMut) -> io::Result<Option<String>> {
    String::from_utf8(buf.to_vec())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
        .map(Some)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut as _;

    use super::*;

    #[test]
    fn lines_decoder() {
        let mut codec = LinesCodec;
        let buf = &mut BytesMut::new();
        buf.reserve(200);
        buf.put_slice(b"\nline 1\nline 2\r\nline 3\n\r\n\r");
        assert_eq!("", codec.decode(buf).unwrap().unwrap());
        assert_eq!("line 1", codec.decode(buf).unwrap().unwrap());
        assert_eq!("line 2", codec.decode(buf).unwrap().unwrap());
        assert_eq!("line 3", codec.decode(buf).unwrap().unwrap());
        assert_eq!("", codec.decode(buf).unwrap().unwrap());
        assert_eq!(None, codec.decode(buf).unwrap());
        assert_eq!(None, codec.decode_eof(buf).unwrap());
        buf.put_slice(b"k");
        assert_eq!(None, codec.decode(buf).unwrap());
        assert_eq!("\rk", codec.decode_eof(buf).unwrap().unwrap());
        assert_eq!(None, codec.decode(buf).unwrap());
        assert_eq!(None, codec.decode_eof(buf).unwrap());
    }
}
