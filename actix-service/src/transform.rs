use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use alloc::{rc::Rc, sync::Arc};
use futures_core::ready;
use pin_project_lite::pin_project;

use crate::transform_err::TransformMapInitErr;
use crate::{IntoServiceFactory, Service, ServiceFactory};

/// Apply transform to a service.
pub fn apply<T, S, I, Req>(t: T, factory: I) -> ApplyTransform<T, S, Req>
where
    I: IntoServiceFactory<S, Req>,
    S: ServiceFactory<Req>,
    T: Transform<S::Service, Req, InitError = S::InitError>,
{
    ApplyTransform::new(t, factory.into_factory())
}

/// The `Transform` trait defines the interface of a service factory that wraps inner service
/// during construction.
///
/// Transform(middleware) wraps inner service and runs during
/// inbound and/or outbound processing in the request/response lifecycle.
/// It may modify request and/or response.
///
/// For example, timeout transform:
///
/// ```ignore
/// pub struct Timeout<S> {
///     service: S,
///     timeout: Duration,
/// }
///
/// impl<S> Service for Timeout<S>
/// where
///     S: Service,
/// {
///     type Request = S::Request;
///     type Response = S::Response;
///     type Error = TimeoutError<S::Error>;
///     type Future = TimeoutServiceResponse<S>;
///
///     actix_service::forward_ready!(service);
///
///     fn call(&self, req: S::Request) -> Self::Future {
///         TimeoutServiceResponse {
///             fut: self.service.call(req),
///             sleep: Delay::new(clock::now() + self.timeout),
///         }
///     }
/// }
/// ```
///
/// Timeout service in above example is decoupled from underlying service implementation
/// and could be applied to any service.
///
/// The `Transform` trait defines the interface of a Service factory. `Transform`
/// is often implemented for middleware, defining how to construct a
/// middleware Service. A Service that is constructed by the factory takes
/// the Service that follows it during execution as a parameter, assuming
/// ownership of the next Service.
///
/// Factory for `Timeout` middleware from the above example could look like this:
///
/// ```ignore
/// pub struct TimeoutTransform {
///     timeout: Duration,
/// }
///
/// impl<S> Transform<S> for TimeoutTransform
/// where
///     S: Service,
/// {
///     type Request = S::Request;
///     type Response = S::Response;
///     type Error = TimeoutError<S::Error>;
///     type InitError = S::Error;
///     type Transform = Timeout<S>;
///     type Future = Ready<Result<Self::Transform, Self::InitError>>;
///
///     fn new_transform(&self, service: S) -> Self::Future {
///         ok(TimeoutService {
///             service,
///             timeout: self.timeout,
///         })
///     }
/// }
/// ```
pub trait Transform<S, Req> {
    /// Responses given by the service.
    type Response;

    /// Errors produced by the service.
    type Error;

    /// The `TransformService` value created by this factory
    type Transform: Service<Req, Response = Self::Response, Error = Self::Error>;

    /// Errors produced while building a transform service.
    type InitError;

    /// The future response value.
    type Future: Future<Output = Result<Self::Transform, Self::InitError>>;

    /// Creates and returns a new Transform component, asynchronously
    fn new_transform(&self, service: S) -> Self::Future;

    /// Map this transform's factory error to a different error,
    /// returning a new transform service factory.
    fn map_init_err<F, E>(self, f: F) -> TransformMapInitErr<Self, S, Req, F, E>
    where
        Self: Sized,
        F: Fn(Self::InitError) -> E + Clone,
    {
        TransformMapInitErr::new(self, f)
    }
}

impl<T, S, Req> Transform<S, Req> for Rc<T>
where
    T: Transform<S, Req>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Transform = T::Transform;
    type InitError = T::InitError;
    type Future = T::Future;

    fn new_transform(&self, service: S) -> T::Future {
        self.as_ref().new_transform(service)
    }
}

impl<T, S, Req> Transform<S, Req> for Arc<T>
where
    T: Transform<S, Req>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Transform = T::Transform;
    type InitError = T::InitError;
    type Future = T::Future;

    fn new_transform(&self, service: S) -> T::Future {
        self.as_ref().new_transform(service)
    }
}

/// `Apply` transform to new service
pub struct ApplyTransform<T, S, Req>(Rc<(T, S)>, PhantomData<Req>);

impl<T, S, Req> ApplyTransform<T, S, Req>
where
    S: ServiceFactory<Req>,
    T: Transform<S::Service, Req, InitError = S::InitError>,
{
    /// Create new `ApplyTransform` new service instance
    fn new(t: T, service: S) -> Self {
        Self(Rc::new((t, service)), PhantomData)
    }
}

impl<T, S, Req> Clone for ApplyTransform<T, S, Req> {
    fn clone(&self) -> Self {
        ApplyTransform(self.0.clone(), PhantomData)
    }
}

impl<T, S, Req> ServiceFactory<Req> for ApplyTransform<T, S, Req>
where
    S: ServiceFactory<Req>,
    T: Transform<S::Service, Req, InitError = S::InitError>,
{
    type Response = T::Response;
    type Error = T::Error;

    type Config = S::Config;
    type Service = T::Transform;
    type InitError = T::InitError;
    type Future = ApplyTransformFuture<T, S, Req>;

    fn new_service(&self, cfg: S::Config) -> Self::Future {
        ApplyTransformFuture {
            store: self.0.clone(),
            state: ApplyTransformFutureState::A {
                fut: self.0.as_ref().1.new_service(cfg),
            },
        }
    }
}

pin_project! {
    pub struct ApplyTransformFuture<T, S, Req>
    where
        S: ServiceFactory<Req>,
        T: Transform<S::Service, Req, InitError = S::InitError>,
    {
        store: Rc<(T, S)>,
        #[pin]
        state: ApplyTransformFutureState<T, S, Req>,
    }
}

pin_project! {
    #[project = ApplyTransformFutureStateProj]
    pub enum ApplyTransformFutureState<T, S, Req>
    where
        S: ServiceFactory<Req>,
        T: Transform<S::Service, Req, InitError = S::InitError>,
    {
        A { #[pin] fut: S::Future },
        B { #[pin] fut: T::Future },
    }
}

impl<T, S, Req> Future for ApplyTransformFuture<T, S, Req>
where
    S: ServiceFactory<Req>,
    T: Transform<S::Service, Req, InitError = S::InitError>,
{
    type Output = Result<T::Transform, T::InitError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            ApplyTransformFutureStateProj::A { fut } => {
                let srv = ready!(fut.poll(cx))?;
                let fut = this.store.0.new_transform(srv);
                this.state.set(ApplyTransformFutureState::B { fut });
                self.poll(cx)
            }
            ApplyTransformFutureStateProj::B { fut } => fut.poll(cx),
        }
    }
}
