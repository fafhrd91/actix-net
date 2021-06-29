use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, Criterion};

const INPUT: &str = "line 1\nline 2\r\nline 3\n\r\n\r";

fn bench_lines_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("lines codec");

    group.bench_function("actix", |b| {
        b.iter(|| {
            use actix_codec::Decoder as _;

            let mut codec = actix_codec::LinesCodec;
            let mut buf = BytesMut::from(INPUT);
            while let Ok(Some(_bytes)) = codec.decode_eof(&mut buf) {}
        });
    });

    group.bench_function("tokio", |b| {
        b.iter(|| {
            use tokio_util::codec::Decoder as _;

            let mut codec = tokio_util::codec::LinesCodec::new();
            let mut buf = BytesMut::from(INPUT);
            while let Ok(Some(_bytes)) = codec.decode_eof(&mut buf) {}
        });
    });

    group.finish();
}

criterion_group!(benches, bench_lines_codec);
criterion_main!(benches);
