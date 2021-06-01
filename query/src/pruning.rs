//! Implementation of statistics based pruning
use arrow::{array::ArrayRef, datatypes::SchemaRef};
use data_types::partition_metadata::{ColumnSummary, Statistics, TableSummary};
use datafusion::{
    logical_plan::Expr,
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    scalar::ScalarValue,
};
use observability_deps::tracing::{debug, trace};

use crate::predicate::Predicate;

/// Trait for an object (designed to be a Chunk) which can provide
/// sufficient information to prune
pub trait Prunable: Sized {
    /// Return a summary of the data in this [`Prunable`]
    fn summary(&self) -> &TableSummary;

    /// return the schema of the data in this [`Prunable`]
    fn schema(&self) -> SchemaRef;
}

/// Something that cares to be notified when pruning of chunks occurs
pub trait PruningObserver {
    type Observed;

    /// Called when the specified chunk was pruned from observation
    fn was_pruned(&self, _chunk: &Self::Observed) {}

    /// Called when no pruning can happen at all for some reason
    fn could_not_prune(&self, _reason: &str) {}

    /// Called when the specified chunk could not be pruned, for some reason
    fn could_not_prune_chunk(&self, _chunk: &Self::Observed, _reason: &str) {}
}

/// Given a Vec of prunable items, returns a possibly smaller set
/// filtering those that can not pass the predicate.
pub fn prune_chunks<C, P, O>(observer: &O, summaries: Vec<C>, predicate: &Predicate) -> Vec<C>
where
    C: AsRef<P>,
    P: Prunable,
    O: PruningObserver<Observed = P>,
{
    let num_chunks = summaries.len();
    debug!(num_chunks, %predicate, "Pruning chunks");

    let filter_expr = match predicate.filter_expr() {
        Some(expr) => expr,
        None => {
            observer.could_not_prune("No expression on predicate");
            return summaries;
        }
    };

    // TODO: performance optimization: batch the chunk pruning by
    // grouping the chunks with the same types for all columns
    // together and then creating a single PruningPredicate for each
    // group.
    let pruned_summaries: Vec<_> = summaries
        .into_iter()
        .filter(|c| must_keep(observer, c.as_ref(), &filter_expr))
        .collect();

    debug!(
        num_chunks,
        num_pruned_chunks = pruned_summaries.len(),
        "Pruned chunks"
    );
    pruned_summaries
}

/// returns true if rows in chunk may pass the predicate
fn must_keep<P, O>(observer: &O, chunk: &P, filter_expr: &Expr) -> bool
where
    P: Prunable,
    O: PruningObserver<Observed = P>,
{
    trace!(?filter_expr, schema=?chunk.schema(), "creating pruning predicate");

    let pruning_predicate = match PruningPredicate::try_new(filter_expr, chunk.schema()) {
        Ok(p) => p,
        Err(e) => {
            observer.could_not_prune_chunk(chunk, "Can not create pruning predicate");
            trace!(%e, ?filter_expr, "Can not create pruning predicate");
            return true;
        }
    };

    let statistics = PrunableStats {
        summary: chunk.summary(),
    };

    match pruning_predicate.prune(&statistics) {
        Ok(results) => {
            // Boolean array for each row in stats, false if the
            // stats could not pass the predicate
            let must_keep = results[0]; // 0 as PrunableStats returns a single row
            if !must_keep {
                observer.was_pruned(chunk)
            }
            must_keep
        }
        Err(e) => {
            observer.could_not_prune_chunk(chunk, "Can not evaluate pruning predicate");
            trace!(%e, ?filter_expr, "Can not evauate pruning predicate");
            true
        }
    }
}

// struct to implement pruning
struct PrunableStats<'a> {
    summary: &'a TableSummary,
}
impl<'a> PrunableStats<'a> {
    fn column_summary(&self, column: &str) -> Option<&ColumnSummary> {
        self.summary.columns.iter().find(|c| c.name == column)
    }
}

/// Converts stats.min and an appropriate `ScalarValue`
fn min_to_scalar(stats: &Statistics) -> Option<ScalarValue> {
    match stats {
        Statistics::I64(v) => v.min.map(ScalarValue::from),
        Statistics::U64(v) => v.min.map(ScalarValue::from),
        Statistics::F64(v) => v.min.map(ScalarValue::from),
        Statistics::Bool(v) => v.min.map(ScalarValue::from),
        Statistics::String(v) => v.min.as_ref().map(|s| s.as_str()).map(ScalarValue::from),
    }
}

/// Converts stats.max to an appropriate `ScalarValue`
fn max_to_scalar(stats: &Statistics) -> Option<ScalarValue> {
    match stats {
        Statistics::I64(v) => v.max.map(ScalarValue::from),
        Statistics::U64(v) => v.max.map(ScalarValue::from),
        Statistics::F64(v) => v.max.map(ScalarValue::from),
        Statistics::Bool(v) => v.max.map(ScalarValue::from),
        Statistics::String(v) => v.max.as_ref().map(|s| s.as_str()).map(ScalarValue::from),
    }
}

impl<'a> PruningStatistics for PrunableStats<'a> {
    fn min_values(&self, column: &str) -> Option<ArrayRef> {
        self.column_summary(column)
            .and_then(|c| min_to_scalar(&c.stats))
            .map(|s| s.to_array_of_size(1))
    }

    fn max_values(&self, column: &str) -> Option<ArrayRef> {
        self.column_summary(column)
            .and_then(|c| max_to_scalar(&c.stats))
            .map(|s| s.to_array_of_size(1))
    }

    fn num_containers(&self) -> usize {
        // We don't (yet) group multiple table summaries into a single
        // object, so we are always evaluating the pruning predicate
        // on a single chunk at a time
        1
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, fmt, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema};
    use data_types::partition_metadata::{ColumnSummary, StatValues, Statistics};
    use datafusion::logical_plan::{col, lit};

    use crate::predicate::PredicateBuilder;

    use super::*;

    #[test]
    fn test_empty() {
        test_helpers::maybe_start_logging();
        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1"));

        let predicate = PredicateBuilder::new().build();
        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert_eq!(
            observer.events(),
            vec!["Could not prune: No expression on predicate"]
        );
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_pruned_f64() {
        test_helpers::maybe_start_logging();
        // column1 > 100.0 where
        //   c1: [0.0, 10.0] --> pruned
        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_f64_column("column1", Some(0.0), Some(10.0)));

        // column1 > 100 where
        //   c1: [0, 10] --> pruned
        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100.0)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);
        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_i64() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned

        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_i64_column("column1", Some(0), Some(10)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_u64() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned

        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_u64_column("column1", Some(0), Some(10)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    // Ignore tests as the pruning predicate can't be created --
    // (maybe boolean predicates not supported in DF?)
    #[ignore]
    fn test_pruned_bool() {
        test_helpers::maybe_start_logging();
        // column1 where
        //   c1: [false, true] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1").with_bool_column(
            "column1",
            Some(false),
            Some(true),
        ));

        let predicate = PredicateBuilder::new().add_expr(col("column1")).build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_string() {
        test_helpers::maybe_start_logging();
        // column1 > "z" where
        //   c1: ["a", "q"] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1").with_string_column(
            "column1",
            Some("a"),
            Some("q"),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit("z")))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_not_pruned_f64() {
        test_helpers::maybe_start_logging();
        // column1 < 100.0 where
        //   c1: [0.0, 10.0] --> not pruned
        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_f64_column("column1", Some(0.0), Some(10.0)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100.0)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);
        assert!(observer.events().is_empty());
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_i64() {
        test_helpers::maybe_start_logging();
        // column1 < 100 where
        //   c1: [0, 10] --> not pruned

        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_i64_column("column1", Some(0), Some(10)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_u64() {
        test_helpers::maybe_start_logging();
        // column1 < 100 where
        //   c1: [0, 10] --> not pruned

        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_u64_column("column1", Some(0), Some(10)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    // Ignore tests as the pruning predicate can't be created --
    // (maybe boolean predicates not supported in DF?)
    #[ignore]
    fn test_not_pruned_bool() {
        test_helpers::maybe_start_logging();
        // column1
        //   c1: [false, false] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1").with_bool_column(
            "column1",
            Some(false),
            Some(false),
        ));

        let predicate = PredicateBuilder::new().add_expr(col("column1")).build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_string() {
        test_helpers::maybe_start_logging();
        // column1 < "z" where
        //   c1: ["a", "q"] --> not pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1").with_string_column(
            "column1",
            Some("a"),
            Some("q"),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit("z")))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_pruned_null() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [Null, 10] --> pruned
        //   c2: [0, Null] --> not pruned
        //   c3: [Null, Null] --> pruned (only nulls in chunk 3)
        //   c4: Null --> not pruned (no stastics at all)

        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1").with_i64_column("column1", None, Some(10)));

        let c2 = Arc::new(TestPrunable::new("chunk2").with_i64_column("column1", Some(0), None));

        let c3 = Arc::new(TestPrunable::new("chunk3").with_i64_column("column1", None, None));

        let c4 = Arc::new(TestPrunable::new("chunk4").with_i64_column_no_stats("column1"));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4], &predicate);

        // DF Bug: c3 sould be pruned (as min=max=NULL means it has only NULL values in it)
        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert_eq!(names(&pruned), vec!["chunk2", "chunk3", "chunk4"]);
    }

    #[test]
    fn test_pruned_multi_chunk() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned
        //   c2: [0, 1000] --> not pruned
        //   c3: [10, 20] --> pruned
        //   c4: [None, None] --> not pruned
        //   c5: [10, None] --> not pruned
        //   c6: [None, 10] --> pruned

        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_i64_column("column1", Some(0), Some(10)));

        let c2 =
            Arc::new(TestPrunable::new("chunk2").with_i64_column("column1", Some(0), Some(1000)));

        let c3 =
            Arc::new(TestPrunable::new("chunk3").with_i64_column("column1", Some(10), Some(20)));

        let c4 = Arc::new(TestPrunable::new("chunk4").with_i64_column("column1", None, None));

        let c5 = Arc::new(TestPrunable::new("chunk5").with_i64_column("column1", Some(10), None));

        let c6 = Arc::new(TestPrunable::new("chunk6").with_i64_column("column1", None, Some(20)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4, c5, c6], &predicate);

        assert_eq!(
            observer.events(),
            vec!["chunk1: Pruned", "chunk3: Pruned", "chunk6: Pruned"]
        );
        assert_eq!(names(&pruned), vec!["chunk2", "chunk4", "chunk5"]);
    }

    #[test]
    fn test_pruned_different_schema() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: column1 [0, 100], column2 [0, 4] --> pruned (in range, column2 ignored)
        //   c2: column1 [0, 1000], column2 [0, 4] --> not pruned (in range, column2 ignored)
        //   c3: None, column2 [0, 4] --> not pruned (no stats for column1)
        let observer = TestObserver::new();
        let c1 = Arc::new(
            TestPrunable::new("chunk1")
                .with_i64_column("column1", Some(0), Some(100))
                .with_i64_column("column2", Some(0), Some(4)),
        );

        let c2 = Arc::new(
            TestPrunable::new("chunk2")
                .with_i64_column("column1", Some(0), Some(1000))
                .with_i64_column("column2", Some(0), Some(4)),
        );

        let c3 = Arc::new(TestPrunable::new("chunk3").with_i64_column("column2", Some(0), Some(4)));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3], &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Pruned",
                "chunk3: Could not prune chunk: Can not evaluate pruning predicate"
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk2", "chunk3"]);
    }

    #[test]
    fn test_pruned_multi_column() {
        test_helpers::maybe_start_logging();
        // column1 > 100 AND column2 < 5 where
        //   c1: column1 [0, 1000], column2 [0, 4] --> not pruned (both in range)
        //   c2: column1 [0, 10], column2 [0, 4] --> pruned (column1 and column2 out of range)
        //   c3: column1 [0, 10], column2 [5, 10] --> pruned (column1 out of range, column2 in of range)
        //   c4: column1 [1000, 2000], column2 [0, 4] --> pruned (column1 out of range, column2 in range)
        //   c5: column1 [0, 10], column2 Null --> not pruned (column1 out of range, but column2 has no stats)
        //   c6: column1 Null, column2 [0, 4] --> not pruned (column1 has no stats, column2 out of range)

        let observer = TestObserver::new();
        let c1 = Arc::new(
            TestPrunable::new("chunk1")
                .with_i64_column("column1", Some(0), Some(1000))
                .with_i64_column("column2", Some(0), Some(4)),
        );

        let c2 = Arc::new(
            TestPrunable::new("chunk2")
                .with_i64_column("column1", Some(0), Some(10))
                .with_i64_column("column2", Some(0), Some(4)),
        );

        let c3 = Arc::new(
            TestPrunable::new("chunk3")
                .with_i64_column("column1", Some(0), Some(10))
                .with_i64_column("column2", Some(5), Some(10)),
        );

        let c4 = Arc::new(
            TestPrunable::new("chunk4")
                .with_i64_column("column1", Some(1000), Some(2000))
                .with_i64_column("column2", Some(0), Some(4)),
        );

        let c5 = Arc::new(
            TestPrunable::new("chunk5")
                .with_i64_column("column1", Some(0), Some(10))
                .with_i64_column_no_stats("column2"),
        );

        let c6 = Arc::new(
            TestPrunable::new("chunk6")
                .with_i64_column_no_stats("column1")
                .with_i64_column("column2", Some(0), Some(4)),
        );

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)).and(col("column2").lt(lit(5))))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4, c5, c6], &predicate);

        // DF BUG: c4 should be pruned (as column1 > 100 can not be true, even though column2 < 5 can be)
        // DF BUG: c5 should not be pruned (column1 > 100 can not be true, but column2 might have values that could rule it out)
        assert_eq!(
            observer.events(),
            vec!["chunk2: Pruned", "chunk3: Pruned", "chunk5: Pruned"]
        );
        assert_eq!(names(&pruned), vec!["chunk1", "chunk4", "chunk6"]);
    }

    #[test]
    fn test_pruned_incompatible_types() {
        test_helpers::maybe_start_logging();
        // Ensure pruning doesn't error / works when some chunks
        // return stats of incompatible types

        // column1 < 100
        //   c1: column1 ["0", "9"] --> not pruned (types are different)
        //   c2: column1 ["1000", "2000"] --> not pruned (types are still different)
        //   c3: column1 [1000, 2000] --> pruned (types are correct)

        let observer = TestObserver::new();
        let c1 = Arc::new(TestPrunable::new("chunk1").with_string_column(
            "column1",
            Some("0"),
            Some("9"),
        ));

        let c2 = Arc::new(TestPrunable::new("chunk2").with_string_column(
            "column1",
            Some("1000"),
            Some("2000"),
        ));

        let c3 = Arc::new(TestPrunable::new("chunk3").with_i64_column(
            "column1",
            Some(1000),
            Some(2000),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3], &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Could not prune chunk: Can not create pruning predicate",
                "chunk2: Could not prune chunk: Can not create pruning predicate",
                "chunk3: Pruned",
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk1", "chunk2"]);
    }

    #[test]
    fn test_pruned_different_types() {
        test_helpers::maybe_start_logging();
        // Ensure pruning works even when different chunks have
        // different types for the columns

        // column1 < 100
        //   c1: column1 [0i64, 1000i64]  --> not pruned (in range)
        //   c2: column1 [0u64, 1000u64] --> not pruned (note types are different)
        //   c3: column1 [1000i64, 2000i64] --> pruned (out of range)
        //   c4: column1 [1000u64, 2000u64] --> pruned (types are different)

        let observer = TestObserver::new();
        let c1 =
            Arc::new(TestPrunable::new("chunk1").with_i64_column("column1", Some(0), Some(1000)));

        let c2 =
            Arc::new(TestPrunable::new("chunk2").with_u64_column("column1", Some(0), Some(1000)));

        let c3 = Arc::new(TestPrunable::new("chunk3").with_i64_column(
            "column1",
            Some(1000),
            Some(2000),
        ));

        let c4 = Arc::new(TestPrunable::new("chunk4").with_u64_column(
            "column1",
            Some(1000),
            Some(2000),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4], &predicate);

        assert_eq!(observer.events(), vec!["chunk3: Pruned", "chunk4: Pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1", "chunk2"]);
    }

    fn names(pruned: &Vec<Arc<TestPrunable>>) -> Vec<&str> {
        pruned.iter().map(|p| p.name.as_str()).collect()
    }

    #[derive(Debug, Default)]
    struct TestObserver {
        events: RefCell<Vec<String>>,
    }

    impl TestObserver {
        fn new() -> Self {
            Self::default()
        }

        fn events(&self) -> Vec<String> {
            self.events.borrow().iter().cloned().collect()
        }
    }

    impl PruningObserver for TestObserver {
        type Observed = TestPrunable;

        fn was_pruned(&self, chunk: &Self::Observed) {
            self.events.borrow_mut().push(format!("{}: Pruned", chunk))
        }

        fn could_not_prune(&self, reason: &str) {
            self.events
                .borrow_mut()
                .push(format!("Could not prune: {}", reason))
        }

        fn could_not_prune_chunk(&self, chunk: &Self::Observed, reason: &str) {
            self.events
                .borrow_mut()
                .push(format!("{}: Could not prune chunk: {}", chunk, reason))
        }
    }

    #[derive(Debug, Clone)]
    struct TestPrunable {
        name: String,
        summary: TableSummary,
        schema: SchemaRef,
    }

    /// Implementation of creating a new column with statitics for TestPrunable
    macro_rules! impl_with_column {
        ($SELF:expr, $COLUMN_NAME:expr, $MIN:expr, $MAX:expr, $DATA_TYPE:ident, $STAT_TYPE:ident) => {{
            let Self {
                name,
                summary,
                schema,
            } = $SELF;
            let column_name = $COLUMN_NAME.into();
            let new_self = Self {
                name,
                schema: Self::add_field_to_schema(&column_name, schema, DataType::$DATA_TYPE),
                summary: Self::add_column_to_summary(
                    summary,
                    column_name,
                    Statistics::$STAT_TYPE(StatValues {
                        min: $MIN,
                        max: $MAX,
                        count: 42,
                    }),
                ),
            };
            new_self
        }};
    }

    impl TestPrunable {
        fn new(name: impl Into<String>) -> Self {
            let name = name.into();
            let summary = TableSummary::new(&name);
            let schema = Arc::new(Schema::new(vec![]));
            Self {
                name,
                summary,
                schema,
            }
        }

        /// Adds an f64 column named into the schema
        fn with_f64_column(
            self,
            column_name: impl Into<String>,
            min: Option<f64>,
            max: Option<f64>,
        ) -> Self {
            impl_with_column!(self, column_name, min, max, Float64, F64)
        }

        /// Adds an i64 column named into the schema
        fn with_i64_column(
            self,
            column_name: impl Into<String>,
            min: Option<i64>,
            max: Option<i64>,
        ) -> Self {
            impl_with_column!(self, column_name, min, max, Int64, I64)
        }

        /// Adds an i64 column named into the schema, but with no stats
        fn with_i64_column_no_stats(self, column_name: impl AsRef<str>) -> Self {
            let Self {
                name,
                summary,
                schema,
            } = self;
            Self {
                name,
                schema: Self::add_field_to_schema(column_name.as_ref(), schema, DataType::Int64),
                // Note we don't add any stats
                summary,
            }
        }

        /// Adds an u64 column named into the schema
        fn with_u64_column(
            self,
            column_name: impl Into<String>,
            min: Option<u64>,
            max: Option<u64>,
        ) -> Self {
            impl_with_column!(self, column_name, min, max, UInt64, U64)
        }

        /// Adds bool column named into the schema
        fn with_bool_column(
            self,
            column_name: impl Into<String>,
            min: Option<bool>,
            max: Option<bool>,
        ) -> Self {
            impl_with_column!(self, column_name, min, max, Boolean, Bool)
        }

        /// Adds a string column named into the schema
        fn with_string_column(
            self,
            column_name: impl Into<String>,
            min: Option<&str>,
            max: Option<&str>,
        ) -> Self {
            let min = min.map(|v| v.to_string());
            let max = max.map(|v| v.to_string());
            impl_with_column!(self, column_name, min, max, Utf8, String)
        }

        fn add_field_to_schema(
            column_name: &str,
            schema: SchemaRef,
            data_type: DataType,
        ) -> SchemaRef {
            let new_field = Field::new(column_name, data_type, true);
            let fields: Vec<_> = schema
                .fields()
                .iter()
                .cloned()
                .chain(std::iter::once(new_field))
                .collect();

            Arc::new(Schema::new(fields))
        }

        fn add_column_to_summary(
            mut summary: TableSummary,
            column_name: impl Into<String>,
            stats: Statistics,
        ) -> TableSummary {
            summary.columns.push(ColumnSummary {
                name: column_name.into(),
                influxdb_type: None,
                stats,
            });

            summary
        }
    }

    impl fmt::Display for TestPrunable {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl Prunable for TestPrunable {
        fn summary(&self) -> &TableSummary {
            &self.summary
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }
}
