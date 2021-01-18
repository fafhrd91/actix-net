use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use actix_rt::net::TcpStream;
use actix_service::{Service, ServiceFactory};
use either::Either;
use futures_core::future::LocalBoxFuture;

use super::connect::{Address, Connect, Connection};
use super::connector::{TcpConnector, TcpConnectorFactory};
use super::error::ConnectError;
use super::resolve::{Resolver, ResolverFactory};

pub struct ConnectServiceFactory {
    tcp: TcpConnectorFactory,
    resolver: ResolverFactory,
}

impl ConnectServiceFactory {
    /// Construct new ConnectService factory
    pub fn new(resolver: Resolver) -> Self {
        ConnectServiceFactory {
            tcp: TcpConnectorFactory,
            resolver: ResolverFactory::new(resolver),
        }
    }

    /// Construct new service
    pub fn service(&self) -> ConnectService {
        ConnectService {
            tcp: self.tcp.service(),
            resolver: self.resolver.service(),
        }
    }

    /// Construct new tcp stream service
    pub fn tcp_service(&self) -> TcpConnectService {
        TcpConnectService {
            tcp: self.tcp.service(),
            resolver: self.resolver.service(),
        }
    }
}

impl Clone for ConnectServiceFactory {
    fn clone(&self) -> Self {
        ConnectServiceFactory {
            tcp: self.tcp,
            resolver: self.resolver.clone(),
        }
    }
}

impl<T: Address> ServiceFactory<Connect<T>> for ConnectServiceFactory {
    type Response = Connection<T, TcpStream>;
    type Error = ConnectError;
    type Config = ();
    type Service = ConnectService;
    type InitError = ();
    type Future = LocalBoxFuture<'static, Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: ()) -> Self::Future {
        let service = self.service();
        Box::pin(async move { Ok(service) })
    }
}

#[derive(Clone)]
pub struct ConnectService {
    tcp: TcpConnector,
    resolver: Resolver,
}

impl<T: Address> Service<Connect<T>> for ConnectService {
    type Response = Connection<T, TcpStream>;
    type Error = ConnectError;
    type Future = ConnectServiceResponse<T>;

    actix_service::always_ready!();

    fn call(&mut self, req: Connect<T>) -> Self::Future {
        ConnectServiceResponse {
            state: ConnectState::Resolve(self.resolver.call(req)),
            tcp: self.tcp,
        }
    }
}

enum ConnectState<T: Address> {
    Resolve(<Resolver as Service<Connect<T>>>::Future),
    Connect(<TcpConnector as Service<Connect<T>>>::Future),
}

impl<T: Address> ConnectState<T> {
    #[allow(clippy::type_complexity)]
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Either<Poll<Result<Connection<T, TcpStream>, ConnectError>>, Connect<T>> {
        match self {
            ConnectState::Resolve(ref mut fut) => match Pin::new(fut).poll(cx) {
                Poll::Pending => Either::Left(Poll::Pending),
                Poll::Ready(Ok(res)) => Either::Right(res),
                Poll::Ready(Err(err)) => Either::Left(Poll::Ready(Err(err))),
            },
            ConnectState::Connect(ref mut fut) => Either::Left(Pin::new(fut).poll(cx)),
        }
    }
}

pub struct ConnectServiceResponse<T: Address> {
    state: ConnectState<T>,
    tcp: TcpConnector,
}

impl<T: Address> Future for ConnectServiceResponse<T> {
    type Output = Result<Connection<T, TcpStream>, ConnectError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = match self.state.poll(cx) {
            Either::Right(res) => {
                self.state = ConnectState::Connect(self.tcp.call(res));
                self.state.poll(cx)
            }
            Either::Left(res) => return res,
        };

        match res {
            Either::Left(res) => res,
            Either::Right(_) => panic!(),
        }
    }
}

#[derive(Clone)]
pub struct TcpConnectService {
    tcp: TcpConnector,
    resolver: Resolver,
}

impl<T: Address + 'static> Service<Connect<T>> for TcpConnectService {
    type Response = TcpStream;
    type Error = ConnectError;
    type Future = TcpConnectServiceResponse<T>;

    actix_service::always_ready!();

    fn call(&mut self, req: Connect<T>) -> Self::Future {
        TcpConnectServiceResponse {
            state: TcpConnectState::Resolve(self.resolver.call(req)),
            tcp: self.tcp,
        }
    }
}

enum TcpConnectState<T: Address> {
    Resolve(<Resolver as Service<Connect<T>>>::Future),
    Connect(<TcpConnector as Service<Connect<T>>>::Future),
}

impl<T: Address> TcpConnectState<T> {
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Either<Poll<Result<TcpStream, ConnectError>>, Connect<T>> {
        match self {
            TcpConnectState::Resolve(ref mut fut) => match Pin::new(fut).poll(cx) {
                Poll::Pending => (),
                Poll::Ready(Ok(res)) => return Either::Right(res),
                Poll::Ready(Err(err)) => return Either::Left(Poll::Ready(Err(err))),
            },
            TcpConnectState::Connect(ref mut fut) => {
                if let Poll::Ready(res) = Pin::new(fut).poll(cx) {
                    return match res {
                        Ok(conn) => Either::Left(Poll::Ready(Ok(conn.into_parts().0))),
                        Err(err) => Either::Left(Poll::Ready(Err(err))),
                    };
                }
            }
        }
        Either::Left(Poll::Pending)
    }
}

pub struct TcpConnectServiceResponse<T: Address> {
    state: TcpConnectState<T>,
    tcp: TcpConnector,
}

impl<T: Address> Future for TcpConnectServiceResponse<T> {
    type Output = Result<TcpStream, ConnectError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = match self.state.poll(cx) {
            Either::Right(res) => {
                self.state = TcpConnectState::Connect(self.tcp.call(res));
                self.state.poll(cx)
            }
            Either::Left(res) => return res,
        };

        match res {
            Either::Left(res) => res,
            Either::Right(_) => panic!(),
        }
    }
}
