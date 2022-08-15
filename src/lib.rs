use std::str::FromStr;

use opentelemetry::{global, Context, ContextGuard};
use opentelemetry::propagation::{Extractor, Injector};

use tonic::metadata::{MetadataKey, KeyRef, MetadataMap};
use tonic::Request;

// extend tracing::Span with context()
use tracing_opentelemetry::OpenTelemetrySpanExt;


pub struct MetadataInjector<'a>(&'a mut MetadataMap);

impl<'a> Injector for MetadataInjector<'a> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = MetadataKey::from_str(key) {
            if let Ok(val) = value.parse() {
                self.0.insert(key, val);
            }
        }
    }
}


pub struct MetadataExtractor<'a>(&'a MetadataMap);

impl<'a> Extractor for MetadataExtractor<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                KeyRef::Ascii(v) => v.as_str(),
                KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}


// pre-requisite:
// global::set_text_map_propagator(TraceContextPropagator::new());
pub fn tracing_parent_span_from_req<T>(request: &Request<T>){
		let cx = global::get_text_map_propagator(|propagator| {
				propagator.extract(&MetadataExtractor(request.metadata()))
		});

		tracing::Span::current().set_parent(cx);
}

// pre-requisite:
// global::set_text_map_propagator(TraceContextPropagator::new());
pub fn tracing_current_span_to_req<T>(request: &mut Request<T>){
		let cx = tracing::Span::current().context();
		global::get_text_map_propagator(|propagator| {
				propagator.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()))
		});
}

// pre-requisite:
// global::set_text_map_propagator(TraceContextPropagator::new());
// context is bind to thread, not like tracing::Span
// need to hold returned guard(e.g. let _xx = ) for this new context to take effect
// context will restore when guard dropped
pub fn otel_thread_cx_from_req<T>(request: &Request<T>)  -> ContextGuard {
		let cx = global::get_text_map_propagator(|propagator| {
				propagator.extract(&MetadataExtractor(request.metadata()))
		});
		cx.attach()
}

// pre-requisite:
// global::set_text_map_propagator(TraceContextPropagator::new());
pub fn otel_thread_cx_to_req<T>(request: &mut Request<T>){
		let cx = Context::current();
		global::get_text_map_propagator(|propagator| {
				propagator.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()))
		});
}

		
#[cfg(test)]
mod tests {
		use opentelemetry::{global, Context};
		use opentelemetry::sdk::{
				propagation::TraceContextPropagator,
				export::trace::stdout
		};
		use opentelemetry::trace::{Tracer, TraceContextExt};
		
		use super::MetadataExtractor;

		use super::MetadataInjector;

    #[test]
    fn inject() {
				global::set_text_map_propagator(TraceContextPropagator::new());
				let tracer = stdout::new_pipeline()
						.install_simple();

				let span = tracer.start("client-span");

				let cx = Context::current_with_span(span);

				let mut request = tonic::Request::new(1);

				global::get_text_map_propagator(|propagator| {
						propagator.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()))
				});
    }

		#[test]
    fn extract() {
				global::set_text_map_propagator(TraceContextPropagator::new());
				let tracer = stdout::new_pipeline()
						.install_simple();

				let request = tonic::Request::new(1);

				let cx = global::get_text_map_propagator(|propagator| {
						propagator.extract(&MetadataExtractor(request.metadata()))
				});

				let span = tracer.start_with_context("server-span", &cx);
				
    }
}
