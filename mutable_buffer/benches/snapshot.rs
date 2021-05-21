use std::{convert::TryFrom, io::Read};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use data_types::server_id::ServerId;
use entry::{test_helpers::lp_to_entries, ClockValue, EntrySequence};
use flate2::read::GzDecoder;
use mutable_buffer::chunk::{Chunk, ChunkMetrics};

#[inline]
fn snapshot_chunk(chunk: &Chunk) {
    let _ = chunk.snapshot();
}

fn chunk(count: usize) -> Chunk {
    // m0 is hard coded into tag_values.lp.gz
    let mut chunk = Chunk::new("m0", ChunkMetrics::new_unregistered());

    let raw = include_bytes!("../../tests/fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    let server_id = ServerId::try_from(1).unwrap();
    let clock_value = ClockValue::try_from(5).unwrap();
    let entry_sequence = EntrySequence::new_from_process_clock(clock_value, server_id);

    for _ in 0..count {
        for entry in lp_to_entries(&lp) {
            for write in entry.partition_writes().iter().flatten() {
                for batch in write.table_batches() {
                    chunk
                        .write_table_batch(
                            entry_sequence,
                            batch,
                        )
                        .unwrap();
                }
            }
        }
    }

    chunk
}

pub fn snapshot_mb(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_mb");
    for count in &[1, 2, 3, 4, 5] {
        let chunk = chunk(*count as _);
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| snapshot_chunk(&chunk));
        });
    }
    group.finish();
}

criterion_group!(benches, snapshot_mb);
criterion_main!(benches);
