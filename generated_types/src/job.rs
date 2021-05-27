use crate::google::{longrunning, protobuf::Any, FieldViolation, FieldViolationExt};
use crate::influxdata::iox::management::v1 as management;
use crate::protobuf_type_url_eq;
use data_types::job::{Job, OperationStatus};
use std::convert::TryFrom;

impl From<Job> for management::operation_metadata::Job {
    fn from(job: Job) -> Self {
        match job {
            Job::Dummy { nanos } => Self::Dummy(management::Dummy { nanos }),
            Job::CloseChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            } => Self::CloseChunk(management::CloseChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }),
            Job::WriteChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            } => Self::WriteChunk(management::WriteChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }),
            Job::WipePreservedCatalog { db_name } => {
                Self::WipePreservedCatalog(management::WipePreservedCatalog { db_name })
            }
        }
    }
}

impl From<management::operation_metadata::Job> for Job {
    fn from(value: management::operation_metadata::Job) -> Self {
        use management::operation_metadata::Job;
        match value {
            Job::Dummy(management::Dummy { nanos }) => Self::Dummy { nanos },
            Job::CloseChunk(management::CloseChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }) => Self::CloseChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            },
            Job::WriteChunk(management::WriteChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }) => Self::WriteChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            },
            Job::WipePreservedCatalog(management::WipePreservedCatalog { db_name }) => {
                Self::WipePreservedCatalog { db_name }
            }
        }
    }
}

impl TryFrom<longrunning::Operation> for data_types::job::Operation {
    type Error = FieldViolation;

    fn try_from(operation: longrunning::Operation) -> Result<Self, Self::Error> {
        let metadata: Any = operation
            .metadata
            .ok_or_else(|| FieldViolation::required("metadata"))?;

        if !protobuf_type_url_eq(&metadata.type_url, management::OPERATION_METADATA) {
            return Err(FieldViolation {
                field: "metadata.type_url".to_string(),
                description: "Unexpected field type".to_string(),
            });
        }

        let meta: management::OperationMetadata =
            prost::Message::decode(metadata.value).field("metadata.value")?;

        let status = match &operation.result {
            None => OperationStatus::Running,
            Some(longrunning::operation::Result::Response(_)) => OperationStatus::Complete,
            Some(longrunning::operation::Result::Error(status)) => {
                if status.code == tonic::Code::Cancelled as i32 {
                    OperationStatus::Cancelled
                } else {
                    OperationStatus::Errored
                }
            }
        };

        Ok(Self {
            id: operation.name.parse().field("name")?,
            task_count: meta.task_count,
            pending_count: meta.pending_count,
            wall_time: std::time::Duration::from_nanos(meta.wall_nanos),
            cpu_time: std::time::Duration::from_nanos(meta.cpu_nanos),
            job: meta.job.map(Into::into),
            status,
        })
    }
}
