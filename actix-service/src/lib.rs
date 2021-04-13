//! See [`Service`] docs for information on this crate's foundational trait.

#![no_std]
#![deny(rust_2018_idioms, nonstandard_style)]
#![allow(clippy::type_complexity)]
#![doc(html_logo_url = "https://actix.rs/img/logo.png")]
#![doc(html_favicon_url = "https://actix.rs/favicon.ico")]

extern crate alloc;

use alloc::{boxed::Box, rc::Rc, sync::Arc};
use core::{
    cell::RefCell,
    future::Future,
    task::{self, Context, Poll},
};

mod and_then;
mod apply;
mod apply_cfg;
pub mod boxed;
mod ext;
mod fn_service;
mod macros;
mod map;
mod map_config;
mod map_err;
mod map_init_err;
mod map_request;
mod pipeline;
mod ready;
mod then;
mod transform;
mod transform_err;

pub use self::apply::{apply_fn, apply_fn_factory};
pub use self::apply_cfg::{apply_cfg, apply_cfg_factory};
pub use self::ext::{ServiceExt, ServiceFactoryExt, TransformExt};
pub use self::fn_service::{fn_factory, fn_factory_with_config, fn_service};
pub use self::map_config::{map_config, unit_config};
pub use self::map_request::{MapRequest, MapRequestMiddleware};
pub use self::pipeline::{pipeline, pipeline_factory, Pipeline, PipelineFactory};
pub use self::transform::{apply, ApplyTransform, Transform};

#[allow(unused_imports)]
use self::ready::{err, ok, ready, Ready};

/// An asynchronous operation from `Request` to a `Response`.
///
/// The `Service` trait models a request/response interaction, receiving requests and returning
/// replies. You can think about a service as a function with one argument that returns some result
/// asynchronously. Conceptually, the operation looks like this:
///
/// ```ignore
/// async fn(Request) -> Result<Response, Err>
/// ```
///
/// The `Service` trait just generalizes this form where each parameter is described as an
/// associated type on the trait. Services can also have mutable state that influence computation.
///
/// `Service` provides a symmetric and uniform API; the same abstractions can be used to represent
/// both clients and servers. Services describe only _transformation_ operations which encourage
/// simple API surfaces. This leads to simpler design of each service, improves test-ability and
/// makes composition easier.
///
/// ```ignore
/// struct MyService;
///
/// impl Service for MyService {
///      type Request = u8;
///      type Response = u64;
///      type Error = MyError;
///      type Future = Pin<Box<Future<Output=Result<Self::Response, Self::Error>>>>;
///
///      fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { ... }
///
///      fn call(&self, req: Self::Request) -> Self::Future { ... }
/// }
/// ```
///
/// Sometimes it is not necessary to implement the Service trait. For example, the above service
/// could be rewritten as a simple function and passed to [fn_service](fn_service()).
///
/// ```ignore
/// async fn my_service(req: u8) -> Result<u64, MyError>;
/// ```
pub trait Service<Req> {
    /// Responses given by the service.
    type Response;

    /// Errors produced by the service when polling readiness or executing call.
    type Error;

    /// The future response value.
    type Future: Future<Output = Result<Self::Response, Self::Error>>;

    /// Returns `Ready` when the service is able to process requests.
    ///
    /// If the service is at capacity, then `Pending` is returned and the task
    /// is notified when the service becomes ready again. This function is
    /// expected to be called while on a task.
    ///
    /// This is a **best effort** implementation. False positives are permitted.
    /// It is permitted for the service to return `Ready` from a `poll_ready`
    /// call and the next invocation of `call` results in an error.
    ///
    /// # Notes
    /// 1. `poll_ready` might be called on a different task to `call`.
    /// 1. In cases of chained services, `.poll_ready()` is called for all services at once.
    fn poll_ready(&self, ctx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Process the request and return the response asynchronously.
    ///
    /// This function is expected to be callable off task. As such,
    /// implementations should take care to not call `poll_ready`. If the
    /// service is at capacity and the request is unable to be handled, the
    /// returned `Future` should resolve to an error.
    ///
    /// Calling `call` without calling `poll_ready` is permitted. The
    /// implementation must be resilient to this fact.
    fn call(&self, req: Req) -> Self::Future;
}

/// Factory for creating `Service`s.
///
/// Acts as a service factory. This is useful for cases where new `Service`s
/// must be produced. One case is a TCP server listener. The listener
/// accepts new TCP streams, obtains a new `Service` using the
/// `ServiceFactory` trait, and uses the new `Service` to process inbound
/// requests on that new TCP stream.
///
/// `Config` is a service factory configuration type.
pub trait ServiceFactory<Req> {
    /// Responses given by the created services.
    type Response;

    /// Errors produced by the created services.
    type Error;

    /// Service factory configuration.
    type Config;

    /// The kind of `Service` created by this factory.
    type Service: Service<Req, Response = Self::Response, Error = Self::Error>;

    /// Errors potentially raised while building a service.
    type InitError;

    /// The future of the `Service` instance.
    type Future: Future<Output = Result<Self::Service, Self::InitError>>;

    /// Create and return a new service asynchronously.
    fn new_service(&self, cfg: Self::Config) -> Self::Future;
}

// TODO: remove implement on mut reference.
impl<'a, S, Req> Service<Req> for &'a mut S
where
    S: Service<Req> + 'a,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        (**self).poll_ready(ctx)
    }

    fn call(&self, request: Req) -> S::Future {
        (**self).call(request)
    }
}

impl<'a, S, Req> Service<Req> for &'a S
where
    S: Service<Req> + 'a,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        (**self).poll_ready(ctx)
    }

    fn call(&self, request: Req) -> S::Future {
        (**self).call(request)
    }
}

impl<S, Req> Service<Req> for Box<S>
where
    S: Service<Req> + ?Sized,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        (**self).poll_ready(ctx)
    }

    fn call(&self, request: Req) -> S::Future {
        (**self).call(request)
    }
}

impl<S, Req> Service<Req> for Rc<S>
where
    S: Service<Req> + ?Sized,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        (**self).poll_ready(ctx)
    }

    fn call(&self, request: Req) -> S::Future {
        (**self).call(request)
    }
}

/// This impl is deprecated since v2 because the `Service` trait now receives shared reference.
impl<S, Req> Service<Req> for RefCell<S>
where
    S: Service<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.borrow().poll_ready(ctx)
    }

    fn call(&self, request: Req) -> S::Future {
        self.borrow().call(request)
    }
}

impl<S, Req> ServiceFactory<Req> for Rc<S>
where
    S: ServiceFactory<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Config = S::Config;
    type Service = S::Service;
    type InitError = S::InitError;
    type Future = S::Future;

    fn new_service(&self, cfg: S::Config) -> S::Future {
        self.as_ref().new_service(cfg)
    }
}

impl<S, Req> ServiceFactory<Req> for Arc<S>
where
    S: ServiceFactory<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Config = S::Config;
    type Service = S::Service;
    type InitError = S::InitError;
    type Future = S::Future;

    fn new_service(&self, cfg: S::Config) -> S::Future {
        self.as_ref().new_service(cfg)
    }
}

/// Trait for types that can be converted to a `Service`
pub trait IntoService<S, Req>
where
    S: Service<Req>,
{
    /// Convert to a `Service`
    fn into_service(self) -> S;
}

/// Trait for types that can be converted to a `ServiceFactory`
pub trait IntoServiceFactory<SF, Req>
where
    SF: ServiceFactory<Req>,
{
    /// Convert `Self` to a `ServiceFactory`
    fn into_factory(self) -> SF;
}

impl<S, Req> IntoService<S, Req> for S
where
    S: Service<Req>,
{
    fn into_service(self) -> S {
        self
    }
}

impl<SF, Req> IntoServiceFactory<SF, Req> for SF
where
    SF: ServiceFactory<Req>,
{
    fn into_factory(self) -> SF {
        self
    }
}

/// Convert object of type `U` to a service `S`
pub fn into_service<I, S, Req>(tp: I) -> S
where
    I: IntoService<S, Req>,
    S: Service<Req>,
{
    tp.into_service()
}
