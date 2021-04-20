use std::sync::Arc;
use tracing::span::{Attributes, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Id, Metadata, Subscriber};
use tracing_core::span::Current;
use tracing_subscriber::registry::{Data, LookupSpan};
use tracing_subscriber::{Registry, Layer};

#[derive(Clone, Debug)]
pub struct SharedRegistry(Arc<Registry>);

impl SharedRegistry {
    pub fn new() -> SharedRegistry {
        SharedRegistry {
            0: Arc::new(Registry::default()),
        }
    }
}

impl Subscriber for SharedRegistry {
    #[inline]
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        self.0.register_callsite(metadata)
    }

    #[inline]
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.0.enabled(metadata)
    }

    #[inline]
    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        self.0.new_span(attrs)
    }

    #[inline]
    fn record(&self, id: &Id, record: &Record<'_>) {
        self.0.record(id, record)
    }

    #[inline]
    fn record_follows_from(&self, span: &Id, follows: &Id) {
        self.0.record_follows_from(span, follows)
    }

    #[inline]
    fn event(&self, event: &Event<'_>) {
        self.0.event(event)
    }

    #[inline]
    fn enter(&self, id: &Id) {
        self.0.enter(id)
    }

    #[inline]
    fn exit(&self, id: &Id) {
        self.0.exit(id)
    }

    #[inline]
    fn clone_span(&self, id: &Id) -> Id {
        self.0.clone_span(id)
    }

    #[inline]
    fn try_close(&self, id: Id) -> bool {
        self.0.try_close(id)
    }

    #[inline]
    fn current_span(&self) -> Current {
        self.0.current_span()
    }
}

impl<'a> LookupSpan<'a> for SharedRegistry {
    type Data = Data<'a>;

    fn span_data(&'a self, id: &Id) -> Option<Data> {
        self.0.span_data(id)
    }
}
