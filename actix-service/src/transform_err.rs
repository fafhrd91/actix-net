use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::Transform;

/// Transform for the `map_init_err` combinator, changing the type of a new
/// transform's init error.
///
/// This is created by the `Transform::map_init_err` method.
pub struct TransformMapInitErr<T, S, Req, F, E> {
    transform: T,
    mapper: F,
    _phantom: PhantomData<(S, Req, E)>,
}

impl<T, S, F, E, Req> TransformMapInitErr<T, S, Req, F, E> {
    pub(crate) fn new(t: T, f: F) -> Self
    where
        T: Transform<S, Req>,
        F: Fn(T::InitError) -> E,
    {
        Self {
            transform: t,
            mapper: f,
            _phantom: PhantomData,
        }
    }
}

impl<T, S, Req, F, E> Clone for TransformMapInitErr<T, S, Req, F, E>
where
    T: Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            transform: self.transform.clone(),
            mapper: self.mapper.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, S, F, E, Req> Transform<S, Req> for TransformMapInitErr<T, S, Req, F, E>
where
    T: Transform<S, Req>,
    F: Fn(T::InitError) -> E + Clone,
{
    type Response = T::Response;
    type Error = T::Error;
    type Transform = T::Transform;

    type InitError = E;
    type Future = TransformMapInitErrFuture<T, S, F, E, Req>;

    fn new_transform(&self, service: S) -> Self::Future {
        TransformMapInitErrFuture {
            fut: self.transform.new_transform(service),
            f: self.mapper.clone(),
        }
    }
}

#[pin_project::pin_project]
pub struct TransformMapInitErrFuture<T, S, F, E, Req>
where
    T: Transform<S, Req>,
    F: Fn(T::InitError) -> E,
{
    #[pin]
    fut: T::Future,
    f: F,
}

impl<T, S, F, E, Req> Future for TransformMapInitErrFuture<T, S, F, E, Req>
where
    T: Transform<S, Req>,
    F: Fn(T::InitError) -> E + Clone,
{
    type Output = Result<T::Transform, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(res) = this.fut.poll(cx) {
            Poll::Ready(res.map_err(this.f))
        } else {
            Poll::Pending
        }
    }
}
