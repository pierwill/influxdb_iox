//! This module contains code for managing the Write Buffer

use data_types::{
    database_rules::{WriteBufferRollover, WriterId},
    ClockValue,
    DatabaseName,
};
use generated_types::wb;
use internal_types::{
    data::ReplicatedWrite,
    entry::SequencedEntry,
};
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};

use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    mem,
    sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use crc32fast::Hasher;
use data_types::database_rules::WriteBufferConfig;
use data_types::write_buffer::{SegmentPersistence, SegmentSummary, WriterSequence};
use observability_deps::tracing::{error, info, warn};
use parking_lot::Mutex;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use tracker::{TaskRegistration, TrackedFutureExt};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Max size limit hit {}", size))]
    MaxSizeLimit { size: u64 },

    #[snafu(display(
        "Unable to drop segment to reduce size below max. Current size {}, segment count {}",
        size,
        segment_count
    ))]
    UnableToDropSegment { size: u64, segment_count: usize },

    #[snafu(display(
        "Sequence from writer {} out of order. Current: {}, incomming {}",
        writer,
        current_sequence,
        incoming_sequence,
    ))]
    SequenceOutOfOrder {
        writer: WriterId,
        current_sequence: u64,
        incoming_sequence: u64,
    },

    #[snafu(display("segment id must be between [1, 1,000,000,000)"))]
    SegmentIdOutOfBounds,

    #[snafu(display("unable to compress segment id {}: {}", segment_id, source))]
    UnableToCompressData {
        segment_id: u64,
        source: snap::Error,
    },

    #[snafu(display("unable to decompress segment data: {}", source))]
    UnableToDecompressData { source: snap::Error },

    #[snafu(display("unable to read checksum: {}", source))]
    UnableToReadChecksum {
        source: std::array::TryFromSliceError,
    },

    #[snafu(display("checksum mismatch for segment"))]
    ChecksumMismatch,

    #[snafu(display("the flatbuffers Segment is invalid: {}", source))]
    InvalidFlatbuffersSegment {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("the flatbuffers size is too small; only found {} bytes", bytes))]
    FlatbuffersSegmentTooSmall { bytes: usize },

    #[snafu(display("the flatbuffers Segment is missing an expected value for {}", field))]
    FlatbuffersMissingField { field: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An in-memory buffer for the Write Buffer. It is split up into segments,
/// which can be persisted to object storage.
#[derive(Debug)]
pub struct Buffer {
    writer_id: WriterId,
    max_size: u64,
    current_size: u64,
    segment_size: u64,
    pub persist: bool,
    open_segment: Segment,
    closed_segments: Vec<Arc<Segment>>,
    rollover_behavior: WriteBufferRollover,
}

impl Buffer {
    pub fn new(
        writer_id: WriterId,
        max_size: u64,
        segment_size: u64,
        rollover_behavior: WriteBufferRollover,
        persist: bool,
    ) -> Self {
        Self {
            writer_id,
            max_size,
            segment_size,
            persist,
            rollover_behavior,
            open_segment: Segment::new(1, writer_id),
            current_size: 0,
            closed_segments: vec![],
        }
    }

    pub fn new_with_config(writer_id: WriterId, config: &WriteBufferConfig) -> Self {
        Self::new(
            writer_id,
            config.buffer_size,
            config.segment_size,
            config.buffer_rollover,
            config.store_segments,
        )
    }

    pub fn append_and_replicate(&mut self, write: Arc<SequencedEntry>) -> Result<()> {
        // append to segment
        self.append(Arc::clone(&write))?;

        // TODO: replicate

        Ok(())
    }

    /// Appends a write onto the buffer, returning the segment if it
    /// has been closed out. If the max size of the buffer would be exceeded
    /// by accepting the write, the oldest (first) of the closed segments
    /// will be dropped, if it is persisted. Otherwise, an error is returned.
    pub fn append(
        &mut self,
        write: Arc<SequencedEntry>,
    ) -> Result<Option<Arc<Segment>>> {
        let write_size = u64::try_from(write.size())
            .expect("appended data must be less than a u64 in length");

        while self.current_size + write_size > self.max_size {
            let oldest_is_persisted = match self.closed_segments.get(0) {
                Some(s) => s.persisted().is_some(),
                None => false,
            };

            if oldest_is_persisted {
                self.remove_oldest_segment();
                continue;
            }

            match self.rollover_behavior {
                WriteBufferRollover::DropIncoming => {
                    warn!(
                        "Write Buffer is full, dropping incoming write \
                        for current segment (segment id: {:?})",
                        self.open_segment.id,
                    );
                    return Ok(None);
                }
                WriteBufferRollover::DropOldSegment => {
                    let oldest_segment_id = self.remove_oldest_segment();
                    warn!(
                        "Write Buffer is full, dropping oldest segment (segment id: {:?})",
                        oldest_segment_id
                    );
                }
                WriteBufferRollover::ReturnError => {
                    return UnableToDropSegment {
                        size: self.current_size,
                        segment_count: self.closed_segments.len(),
                    }
                    .fail();
                }
            }
        }

        let mut closed_segment = None;

        self.current_size += write_size;
        self.open_segment.append(write)?;
        if self.open_segment.size > self.segment_size {
            let next_id = self.open_segment.id + 1;
            let segment = mem::replace(
                &mut self.open_segment,
                Segment::new(next_id, self.writer_id),
            );
            let segment = Arc::new(segment);

            self.closed_segments.push(Arc::clone(&segment));
            closed_segment = Some(segment);
        }

        Ok(closed_segment)
    }

    /// Returns the current size of the buffer.
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// Returns any replicated writes from the given writer ID and sequence
    /// number onward. This will include writes from other writers. The
    /// given writer ID and sequence are to identify from what point to
    /// replay writes. If no write matches the given writer ID and sequence
    /// number, all replicated writes within the buffer will be returned.
    pub fn all_writes_since(&self, since: WriterSequence) -> Vec<Arc<SequencedEntry>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for (&writer_sequence, sequenced_entry) in self.open_segment.sequenced_entries.iter().rev()
        {
            if since <= writer_sequence {
                writes.reverse();
                return writes;
            }

            writes.push(Arc::clone(&sequenced_entry));
        }

        for s in self.closed_segments.iter().rev() {
            for (&writer_sequence, sequenced_entry) in s.sequenced_entries.iter().rev() {
                if since <= writer_sequence {
                    writes.reverse();
                    return writes;
                }

                writes.push(Arc::clone(&sequenced_entry));
            }
        }

        writes.reverse();
        writes
    }

    /// Returns replicated writes from the given writer ID and sequence number
    /// onward. This returns only writes from the passed in writer ID. If no
    /// write matches the given writer ID and sequence number, all
    /// replicated writes within the buffer for that writer will be returned.
    pub fn writes_since(&self, since: WriterSequence) -> Vec<Arc<SequencedEntry>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for (&writer_sequence, sequenced_entry) in self.open_segment.sequenced_entries.iter().rev()
        {
            if writer_sequence.writer_id == since.writer_id {
                if writer_sequence.clock_value <= since.clock_value {
                    writes.reverse();
                    return writes;
                }
                writes.push(Arc::clone(sequenced_entry));
            }
        }

        for s in self.closed_segments.iter().rev() {
            for (&writer_sequence, sequenced_entry) in s.sequenced_entries.iter().rev() {
                if writer_sequence.writer_id == since.writer_id {
                    if writer_sequence.clock_value <= since.clock_value {
                        writes.reverse();
                        return writes;
                    }
                    writes.push(Arc::clone(sequenced_entry));
                }
            }
        }

        writes.reverse();
        writes
    }

    /// Returns a list of segment summaries for stored segments
    pub fn segments(&self, offset: Option<usize>) -> impl Iterator<Item = SegmentSummary> + '_ {
        std::iter::once(&self.open_segment)
            .chain(self.closed_segments.iter().map(|x| x.as_ref()).rev())
            .skip(offset.unwrap_or(0))
            .map(|x| x.summary())
    }

    /// Removes the oldest segment present in the buffer, returning its id
    fn remove_oldest_segment(&mut self) -> u64 {
        let removed_segment = self.closed_segments.remove(0);
        self.current_size -= removed_segment.size;
        removed_segment.id
    }
}

/// Segment is a collection of sequenced entries that can be persisted to
/// object store.
#[derive(Debug)]
pub struct Segment {
    // Do we still want to have a segment ID? Useful for debugging?
    id: u64,
    size: u64,
    sequenced_entries: BTreeMap<WriterSequence, Arc<SequencedEntry>>,
    writer_id: WriterId,
    consistency_high_water: ClockValue,
    // Persistence metadata if segment is persisted
    persisted: Mutex<Option<SegmentPersistence>>,
}

impl Segment {
    fn new(id: u64, writer_id: WriterId) -> Self {
        Self {
            id,
            size: 0,
            sequenced_entries: BTreeMap::new(),
            writer_id,
            consistency_high_water: ClockValue::default(),
            persisted: Mutex::new(None),
        }
    }

    fn min_clock_value(&self) -> ClockValue {
        self.sequenced_entries.iter().next().map(|(k, _v)| k.clock_value).unwrap_or_default()
    }

    fn max_clock_value(&self) -> ClockValue {
        self.sequenced_entries.iter().rev().next().map(|(k, _v)| k.clock_value).unwrap_or_default()
    }

    // appends the write to the segment, keeping track of the summary information
    // about the writer
    fn append(&mut self, write: Arc<SequencedEntry>) -> Result<()> {
        let writer_sequence = WriterSequence {
            clock_value: write.clock_value(),
            writer_id: write.writer_id(),
        };

        let size = write.size();
        let size = u64::try_from(size).expect("appended data must be less than a u64 in length");
        self.size += size;

        self.sequenced_entries.insert(writer_sequence, write);
        Ok(())
    }

    /// sets the persistence metadata for this segment
    pub fn set_persisted(&self, persisted: SegmentPersistence) {
        let mut self_persisted = self.persisted.lock();
        *self_persisted = Some(persisted);
    }

    /// returns persistence metadata for this segment if persisted
    pub fn persisted(&self) -> Option<SegmentPersistence> {
        self.persisted.lock().clone()
    }

    // /// Spawns a tokio task that will continuously try to persist the bytes to
    // /// the given object store location.
    // pub fn persist_bytes_in_background(
    //     &self,
    //     tracker: TaskRegistration,
    //     writer_id: u32,
    //     db_name: &DatabaseName<'_>,
    //     store: Arc<ObjectStore>,
    // ) -> Result<()> {
    //     let data = self.to_file_bytes(writer_id)?;
    //     let location = database_object_store_path(writer_id, db_name, &store);
    //     let location = object_store_path_for_segment(&location, self.id)?;
    //
    //     let len = data.len();
    //     let mut stream_data = std::io::Result::Ok(data.clone());
    //
    //     tokio::task::spawn(
    //         async move {
    //             while let Err(err) = store
    //                 .put(
    //                     &location,
    //                     futures::stream::once(async move { stream_data }),
    //                     Some(len),
    //                 )
    //                 .await
    //             {
    //                 error!("error writing bytes to store: {}", err);
    //                 tokio::time::sleep(tokio::time::Duration::from_secs(
    //                     super::STORE_ERROR_PAUSE_SECONDS,
    //                 ))
    //                 .await;
    //                 stream_data = std::io::Result::Ok(data.clone());
    //             }
    //
    //             // TODO: Mark segment as persisted
    //             info!("persisted data to {}", location.display());
    //         }
    //         .track(tracker),
    //     );
    //
    //     Ok(())
    // }

    // /// converts the segment to its flatbuffer bytes
    // fn fb_bytes(&self, writer_id: u32) -> Vec<u8> {
    //     let mut fbb = flatbuffers::FlatBufferBuilder::new_with_capacity(
    //         usize::try_from(self.size).expect("unable to serialize segment of this size"),
    //     );
    //     let writes = self
    //         .writes
    //         .iter()
    //         .map(|rw| {
    //             let payload = fbb.create_vector_direct(rw.data());
    //             wb::ReplicatedWriteData::create(
    //                 &mut fbb,
    //                 &wb::ReplicatedWriteDataArgs {
    //                     payload: Some(payload),
    //                 },
    //             )
    //         })
    //         .collect::<Vec<flatbuffers::WIPOffset<wb::ReplicatedWriteData<'_>>>>();
    //     let writes = fbb.create_vector(&writes);
    //
    //     let segment = wb::Segment::create(
    //         &mut fbb,
    //         &wb::SegmentArgs {
    //             id: self.id,
    //             writer_id,
    //             writes: Some(writes),
    //         },
    //     );
    //
    //     fbb.finish(segment, None);
    //
    //     let (mut data, idx) = fbb.collapse();
    //     data.split_off(idx)
    // }

    /// returns a summary of the data stored within this segment
    pub fn summary(&self) -> SegmentSummary {
        let persisted = self.persisted.lock().clone();

        SegmentSummary {
            size: self.size,
            persisted,
            sequenced_entries: self.sequenced_entries.keys().cloned().collect(),
        }
    }

    // /// serialize the segment to the bytes to represent it in a file. This
    // /// compresses the flatbuffers payload and writes a crc32 checksum at
    // /// the end.
    // pub fn to_file_bytes(&self, writer_id: u32) -> Result<Bytes> {
    //     let fb_bytes = self.fb_bytes(writer_id);
    //
    //     let mut encoder = snap::raw::Encoder::new();
    //     let mut compressed_data =
    //         encoder
    //             .compress_vec(&fb_bytes)
    //             .context(UnableToCompressData {
    //                 segment_id: self.id,
    //             })?;
    //
    //     let mut hasher = Hasher::new();
    //     hasher.update(&compressed_data);
    //     let checksum = hasher.finalize();
    //
    //     compressed_data.extend_from_slice(&checksum.to_le_bytes());
    //
    //     Ok(Bytes::from(compressed_data))
    // }

    // /// checks the crc32 for the compressed data, decompresses it and
    // /// deserializes it into a Segment struct.
    // pub fn from_file_bytes(data: &[u8]) -> Result<Self> {
    //     if data.len() < std::mem::size_of::<u32>() {
    //         return FlatbuffersSegmentTooSmall { bytes: data.len() }.fail();
    //     }
    //
    //     let (data, checksum) = data.split_at(data.len() - std::mem::size_of::<u32>());
    //     let checksum = u32::from_le_bytes(checksum.try_into().context(UnableToReadChecksum)?);
    //
    //     let mut hasher = Hasher::new();
    //     hasher.update(&data);
    //
    //     if checksum != hasher.finalize() {
    //         return Err(Error::ChecksumMismatch);
    //     }
    //
    //     let mut decoder = snap::raw::Decoder::new();
    //     let data = decoder
    //         .decompress_vec(data)
    //         .context(UnableToDecompressData)?;
    //
    //     // Use verified flatbuffer functionality here
    //     let fb_segment =
    //         flatbuffers::root::<wb::Segment<'_>>(&data).context(InvalidFlatbuffersSegment)?;
    //
    //     let writes = fb_segment
    //         .writes()
    //         .context(FlatbuffersMissingField { field: "writes" })?;
    //     let mut segment = Self::new_with_capacity(fb_segment.id(), writes.len());
    //     for w in writes {
    //         let data = w
    //             .payload()
    //             .context(FlatbuffersMissingField { field: "payload" })?
    //             .to_vec();
    //         let rw = ReplicatedWrite::try_from(data).context(InvalidFlatbuffersSegment)?;
    //
    //         segment.append(Arc::new(rw))?;
    //     }
    //
    //     Ok(segment)
    // }
}

const WRITE_BUFFER_DIR: &str = "wb";
const MAX_SEGMENT_ID: u64 = 999_999_999;
const SEGMENT_FILE_EXTENSION: &str = ".segment";

/// Builds the path for a given segment id, given the root object store path.
/// The path should be where the root of the database is (e.g. 1/my_db/).
fn object_store_path_for_segment<P: ObjectStorePath>(root_path: &P, segment_id: u64) -> Result<P> {
    ensure!(
        segment_id < MAX_SEGMENT_ID && segment_id > 0,
        SegmentIdOutOfBounds
    );

    let millions_place = segment_id / 1_000_000;
    let millions = millions_place * 1_000_000;
    let thousands_place = (segment_id - millions) / 1_000;
    let thousands = thousands_place * 1_000;
    let hundreds_place = segment_id - millions - thousands;

    let mut path = root_path.clone();
    path.push_all_dirs(&[
        WRITE_BUFFER_DIR,
        &format!("{:03}", millions_place),
        &format!("{:03}", thousands_place),
    ]);
    path.set_file_name(format!("{:03}{}", hundreds_place, SEGMENT_FILE_EXTENSION));

    Ok(path)
}

// base location in object store for a given database name
fn database_object_store_path(
    writer_id: u32,
    database_name: &DatabaseName<'_>,
    store: &ObjectStore,
) -> object_store::path::Path {
    let mut path = store.new_path();
    path.push_dir(format!("{}", writer_id));
    path.push_dir(database_name.to_string());
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::database_rules::PartitionTemplate;
    use influxdb_line_protocol::parse_lines;
    use internal_types::data::lines_to_replicated_write;
    use object_store::memory::InMemory;

    #[test]
    fn append_increments_current_size_and_uses_existing_segment() {
        let max = 1 << 32;
        let segment = 1 << 16;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");

        let size = write.data().len() as u64;
        assert_eq!(0, buf.size());
        let segment = buf.append(write).unwrap();
        assert_eq!(size, buf.size());
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert_eq!(size * 2, buf.size());
        assert!(segment.is_none());
    }

    #[test]
    fn append_rolls_over_segment() {
        let max = 1 << 16;
        let segment = 1;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");

        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 1);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");

        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 2);
    }

    #[test]
    fn drops_persisted_segment_when_over_size() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);
        assert!(segment.persisted().is_none());
        segment.set_persisted(SegmentPersistence {
            location: "PLACEHOLDER".to_string(),
            time: Utc::now(),
        });

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);
        assert!(segment.persisted().is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[test]
    fn drops_old_segment_even_if_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::DropOldSegment, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[test]
    fn drops_incoming_write_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::DropIncoming, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[test]
    fn returns_error_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        assert!(buf.append(write).is_err());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[test]
    fn all_writes_since() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = (write.data().len() + 1) as u64;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_replicated_write(2, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let writes = buf.all_writes_since(WriterSequence { id: 0, sequence: 1 });
        assert_eq!(5, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 1));
        assert!(writes[1].equal_to_writer_and_sequence(2, 1));
        assert!(writes[2].equal_to_writer_and_sequence(1, 2));
        assert!(writes[3].equal_to_writer_and_sequence(1, 3));
        assert!(writes[4].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 1, sequence: 1 });
        assert_eq!(4, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 1));
        assert!(writes[1].equal_to_writer_and_sequence(1, 2));
        assert!(writes[2].equal_to_writer_and_sequence(1, 3));
        assert!(writes[3].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 2, sequence: 1 });
        assert_eq!(3, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 2));
        assert!(writes[1].equal_to_writer_and_sequence(1, 3));
        assert!(writes[2].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 1, sequence: 3 });
        assert_eq!(1, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 2, sequence: 2 });
        assert_eq!(0, writes.len());
    }

    #[test]
    fn writes_since() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = (write.data().len() + 1) as u64;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_replicated_write(2, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let writes = buf.writes_since(WriterSequence { id: 0, sequence: 1 });
        assert_eq!(0, writes.len());

        let writes = buf.writes_since(WriterSequence { id: 1, sequence: 0 });
        assert_eq!(3, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 1));
        assert!(writes[1].equal_to_writer_and_sequence(1, 2));
        assert!(writes[2].equal_to_writer_and_sequence(1, 3));

        let writes = buf.writes_since(WriterSequence { id: 1, sequence: 1 });
        assert_eq!(2, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 2));
        assert!(writes[1].equal_to_writer_and_sequence(1, 3));

        let writes = buf.writes_since(WriterSequence { id: 2, sequence: 1 });
        assert_eq!(1, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 2));
    }

    #[test]
    fn returns_error_if_sequence_decreases() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = (write.data().len() + 1) as u64;
        let mut buf = Buffer::new(1, max, segment, WriteBufferRollover::ReturnError, false);

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        assert!(buf.append(write).is_err());
    }

    #[test]
    fn segment_keeps_writer_summaries() {
        let mut segment = Segment::new(1);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(2, 4, "cpu val=1 10");
        segment.append(write).unwrap();

        let summary = segment.writers.get(&1).unwrap();
        assert_eq!(summary.min_clock_value(), 1);
        assert_eq!(summary.max_clock_value(), 2);

        let summary = segment.writers.get(&2).unwrap();
        assert_eq!(summary.min_clock_value(), 1);
        assert_eq!(summary.max_clock_value(), 4);
    }

    #[test]
    fn valid_object_store_path_for_segment() {
        let storage = ObjectStore::new_in_memory(InMemory::new());
        let mut base_path = storage.new_path();
        base_path.push_all_dirs(&["1", "mydb"]);

        let segment_path = object_store_path_for_segment(&base_path, 23).unwrap();
        let mut expected_segment_path = base_path.clone();
        expected_segment_path.push_all_dirs(&["wb", "000", "000"]);
        expected_segment_path.set_file_name("023.segment");
        assert_eq!(segment_path, expected_segment_path);

        let segment_path = object_store_path_for_segment(&base_path, 20_003).unwrap();
        let mut expected_segment_path = base_path.clone();
        expected_segment_path.push_all_dirs(&["wb", "000", "020"]);
        expected_segment_path.set_file_name("003.segment");
        assert_eq!(segment_path, expected_segment_path);

        let segment_path = object_store_path_for_segment(&base_path, 45_010_105).unwrap();
        let mut expected_segment_path = base_path;
        expected_segment_path.push_all_dirs(&["wb", "045", "010"]);
        expected_segment_path.set_file_name("105.segment");
        assert_eq!(segment_path, expected_segment_path);
    }

    #[test]
    fn object_store_path_for_segment_out_of_bounds() {
        let storage = ObjectStore::new_in_memory(InMemory::new());
        let mut base_path = storage.new_path();
        base_path.push_all_dirs(&["1", "mydb"]);

        let segment_path = object_store_path_for_segment(&base_path, 0).err().unwrap();
        matches!(segment_path, Error::SegmentIdOutOfBounds);

        let segment_path = object_store_path_for_segment(&base_path, 23_000_000_000)
            .err()
            .unwrap();
        matches!(segment_path, Error::SegmentIdOutOfBounds);
    }

    #[test]
    fn segment_serialize_deserialize() {
        let id = 1;
        let mut segment = Segment::new(id);
        let writer_id = 2;
        segment
            .append(lp_to_replicated_write(writer_id, 0, "foo val=1 123"))
            .unwrap();
        segment
            .append(lp_to_replicated_write(writer_id, 1, "foo val=2 124"))
            .unwrap();

        let data = segment.to_file_bytes(writer_id).unwrap();
        let recovered_segment = Segment::from_file_bytes(&data).unwrap();

        assert_eq!(segment.id, recovered_segment.id);
        assert_eq!(segment.size, recovered_segment.size);
        assert_eq!(segment.writes, recovered_segment.writes);
    }

    fn lp_to_replicated_write(
        writer_id: u32,
        sequence_number: u64,
        lp: &str,
    ) -> Arc<ReplicatedWrite> {
        let lines: Vec<_> = parse_lines(lp).map(|l| l.unwrap()).collect();
        let partitioner = PartitionTemplate::default();
        Arc::new(lines_to_replicated_write(
            writer_id,
            sequence_number,
            &lines,
            &partitioner,
        ))
    }
}
