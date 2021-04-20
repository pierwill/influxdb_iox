use std::any::TypeId;
use std::cmp::Ordering;
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Record};
use tracing::Id;
use tracing_core::span::Current;
use {
    std::marker::PhantomData,
    tracing_core::{
        metadata::Metadata,
        span,
        subscriber::{Interest, Subscriber},
        Event,
    },
    tracing_subscriber::{layer, registry::LookupSpan, Layer},
};

pub struct ForkingLayer<L, R, S = R> {
    left: L,
    right: R,
    _s: PhantomData<fn(S)>, // TODO is this field necessary?
}

impl<L, R, S> ForkingLayer<L, R, S>
where
    L: Layer<S>,
    R: Layer<S>,
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    pub fn new(left: L, right: R) -> Self {
        ForkingLayer {
            left,
            right,
            _s: PhantomData,
        }
    }
}

impl<L, R, S> Layer<S> for ForkingLayer<L, R, S>
where
    L: Layer<S>,
    R: Layer<S>,
    S: Subscriber,
{
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        let left_interest = self.left.register_callsite(metadata);
        let right_interest = self.right.register_callsite(metadata);
        if left_interest.is_always() || right_interest.is_always() {
            Interest::always()
        } else if left_interest.is_sometimes() || right_interest.is_sometimes() {
            Interest::sometimes()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: layer::Context<'_, S>) -> bool {
        self.left.enabled(metadata, ctx.clone()) || self.right.enabled(metadata, ctx)
    }

    fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        self.left.new_span(attrs, id, ctx.clone());
        self.right.new_span(attrs, id, ctx);
    }

    fn on_record(&self, id: &span::Id, record: &span::Record<'_>, ctx: layer::Context<'_, S>) {
        self.left.on_record(id, record, ctx.clone());
        self.right.on_record(id, record, ctx);
    }

    fn on_follows_from(&self, id: &span::Id, follows: &span::Id, ctx: layer::Context<'_, S>) {
        self.left.on_follows_from(id, follows, ctx.clone());
        self.right.on_follows_from(id, follows, ctx);
    }

    fn on_event(&self, event: &Event<'_>, ctx: layer::Context<'_, S>) {
        self.left.on_event(event, ctx.clone());
        self.right.on_event(event, ctx);
    }

    fn on_enter(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        self.left.on_enter(id, ctx.clone());
        self.right.on_enter(id, ctx);
    }

    fn on_exit(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        self.left.on_exit(id, ctx.clone());
        self.right.on_exit(id, ctx);
    }

    fn on_close(&self, id: span::Id, ctx: layer::Context<'_, S>) {
        self.left.on_close(id.clone(), ctx.clone());
        self.right.on_close(id, ctx);
    }

    fn on_id_change(&self, old_id: &span::Id, new_id: &span::Id, ctx: layer::Context<'_, S>) {
        self.left.on_id_change(old_id, new_id, ctx.clone());
        self.right.on_id_change(old_id, new_id, ctx);
    }
}

struct ForkingSubscriber<L, R> {
    left: L,
    right: R,
}

impl<L, R> Subscriber for ForkingSubscriber<L, R>
where
    L: Subscriber,
    R: Subscriber,
{
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        let left_interest = self.left.register_callsite(metadata);
        let right_interest = self.right.register_callsite(metadata);
        if left_interest.is_always() || right_interest.is_always() {
            Interest::always()
        } else if left_interest.is_sometimes() || right_interest.is_sometimes() {
            Interest::sometimes()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        let left_enabled = self.left.enabled(metadata);
        let right_enabled = self.right.enabled(metadata);
        left_enabled || right_enabled
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        let left_max_level_hint = self.left.max_level_hint();
        let right_max_level_hint = self.right.max_level_hint();
        match (left_max_level_hint, right_max_level_hint) {
            (Some(_), Some(_)) => match left_max_level_hint.cmp(&right_max_level_hint) {
                Ordering::Less => left_max_level_hint,
                _ => right_max_level_hint,
            },
            (Some(_), None) => left_max_level_hint,
            (None, Some(_)) => right_max_level_hint,
            (None, None) => None,
        }
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        todo!()
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        todo!()
    }

    fn record_follows_from(&self, span: &Id, follows: &Id) {
        todo!()
    }

    fn event(&self, event: &Event<'_>) {
        todo!()
    }

    fn enter(&self, span: &Id) {
        todo!()
    }

    fn exit(&self, span: &Id) {
        todo!()
    }

    fn clone_span(&self, id: &Id) -> Id {
        todo!()
    }

    fn drop_span(&self, _id: Id) {
        todo!()
    }

    fn try_close(&self, id: Id) -> bool {
        todo!()
    }

    fn current_span(&self) -> Current {
        todo!()
    }

    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        todo!()
    }
}
