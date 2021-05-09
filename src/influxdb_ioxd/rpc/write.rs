use std::sync::Arc;

use generated_types::{google::FieldViolation, influxdata::iox::write::v1::*};
use influxdb_line_protocol::parse_lines_static;
use observability_deps::tracing::debug;
use server::{ConnectionManager, Server};
use std::fmt::Debug;
use tonic::Response;

use super::error::default_server_error_handler;

/// Implementation of the write service
struct WriteService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

#[tonic::async_trait]
impl<M> write_service_server::WriteService for WriteService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let request = request.into_inner();

        let db_name = request.db_name;
        let lp_data = Arc::<str>::from(request.lp_data);
        let lp_chars = lp_data.len();

        let lines = parse_lines_static(&lp_data)
            .collect::<Result<Vec<_>, influxdb_line_protocol::Error>>()
            .map_err(|e| FieldViolation {
                field: "lp_data".into(),
                description: format!("Invalid Line Protocol: {}", e),
            })?;

        let lp_line_count = lines.len();
        debug!(%db_name, %lp_chars, lp_line_count, "Writing lines into database");

        self.server
            .write_lines(&db_name, lines)
            .await
            .map_err(default_server_error_handler)?;

        let lines_written = lp_line_count as u64;
        Ok(Response::new(WriteResponse { lines_written }))
    }

    async fn write_entry(
        &self,
        request: tonic::Request<WriteEntryRequest>,
    ) -> Result<tonic::Response<WriteEntryResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.entry.is_empty() {
            return Err(FieldViolation::required("entry").into());
        }

        self.server
            .write_entry(&request.db_name, request.entry)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(WriteEntryResponse {}))
    }
}

/// Instantiate the write service
pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    write_service_server::WriteServiceServer::new(WriteService { server })
}
