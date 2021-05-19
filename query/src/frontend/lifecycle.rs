use std::sync::Arc;

use snafu::Snafu;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::{col, PhysicalSortExpr};
use datafusion::physical_plan::{sort::SortExec, ExecutionPlan};
use internal_types::selection::Selection;

use crate::provider::ProviderBuilder;
use crate::{provider, PartitionChunk};
use arrow::error::ArrowError;
use data_types::partition_metadata::TableSummary;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("chunk error: {} {}", chunk_id, source))]
    CheckingChunkPredicate {
        chunk_id: u32,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(context(false))]
    ProviderError { source: provider::Error },

    #[snafu(context(false))]
    DataFusionError { source: DataFusionError },

    #[snafu(context(false))]
    ArrowError { source: ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Plans queries for performing lifecycle actions
#[derive(Debug, Default)]
pub struct LifecyclePlanner {}

impl LifecyclePlanner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn compact_chunks<C: PartitionChunk + 'static>(
        &self,
        table_name: &str,
        chunks: Vec<(Arc<C>, Arc<TableSummary>)>,
        output_chunks: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = 0;

        let mut builder = ProviderBuilder::new("placeholder");
        for (chunk, _) in chunks {
            let schema = chunk
                .table_schema(table_name, Selection::All)
                .map_err(|e| Error::CheckingChunkPredicate {
                    chunk_id: chunk.id(),
                    source: Box::new(e),
                })?;

            builder = builder.add_chunk(chunk, schema)?;
        }
        let provider = builder.build()?;
        let sort_order: Vec<String> = todo!();

        let sort = sort_order
            .into_iter()
            .map(|x| PhysicalSortExpr {
                expr: col(&x),
                options: Default::default(),
            })
            .collect();

        let src = provider.scan(&None, batch_size, &[], None)?;

        let sort = SortExec::try_new(sort, src)?;

        todo!()
    }
}
