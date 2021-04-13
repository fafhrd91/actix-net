use std::{cell::RefCell, future::Future, rc::Rc};

use futures_util::future::{self, FutureExt as _, LocalBoxFuture, TryFutureExt as _};

use super::{Service, Transform};

#[derive(Clone, Debug)]
pub struct MapRequest<F>(pub F);

pub struct MapRequestMiddleware<S, F> {
    service: Rc<RefCell<S>>,
    f: F,
}

impl<S, F, R> Transform<S> for MapRequest<F>
where
    S: Service + 'static,
    F: FnMut(S::Request) -> R + Clone + 'static,
    R: Future<Output = Result<S::Request, S::Error>> + 'static,
{
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Transform = MapRequestMiddleware<S, F>;
    type InitError = ();
    type Future = future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ok(MapRequestMiddleware {
            service: Rc::new(RefCell::new(service)),
            f: self.0.clone(),
        })
    }
}

impl<S, F, R> Service for MapRequestMiddleware<S, F>
where
    S: Service + 'static,
    F: FnMut(S::Request) -> R + Clone + 'static,
    R: Future<Output = Result<S::Request, S::Error>> + 'static,
{
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = LocalBoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(
        &mut self,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.borrow_mut().poll_ready(ctx)
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let mut f = self.f.clone();
        let service = Rc::clone(&self.service);

        f(req)
            .and_then(move |req| service.borrow_mut().call(req))
            .boxed_local()
    }
}
