// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod ingestion_svc;
use hyper::body::Incoming;
use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;

use std::convert::Infallible;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http::{Request, Response, Uri};
use http_body_util::BodyExt;
use http_body_util::combinators::UnsyncBoxBody;
use tower::ServiceExt;

use crate::ingestion;

use super::*;

/// The base prefix that is stripped before dispatching to the Connect service.
pub(crate) const INGEST_BASE: &str = "/restate";

/// The ingress-mounted path prefix under which the Connect service is served.
///
/// Clients use the base URL `.../restate`; Connect appends the canonical RPC
/// path, yielding `/restate/restate.ingestion.IngestionSvc/Ingest`. The server
/// strips the leading `/restate` before handing the request to `ConnectRpcService`.
pub(crate) const INGEST_MOUNT_PREFIX: &str = "/restate/restate.ingestion.IngestionSvc/";

/// Unified response body for both branches.
type RoutedBody = UnsyncBoxBody<Bytes, Infallible>;

#[derive(Clone)]
pub(super) struct IngestRouter<S> {
    inner: S,
    connect: connectrpc::ConnectRpcService,
}

impl<S> IngestRouter<S> {
    pub(crate) fn new<Schemas>(inner: S, schemas: Live<Schemas>) -> Self
    where
        Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        Self {
            inner,
            connect: ingestion_svc::connect_service(schemas),
        }
    }
}

impl<S, Body> tower::Service<Request<Incoming>> for IngestRouter<S>
where
    S: tower::Service<Request<Incoming>, Response = Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    Body: http_body::Body<Data = Bytes, Error = Infallible> + Send + 'static,
{
    type Response = Response<RoutedBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        // Both inner services are always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<Incoming>) -> Self::Future {
        if req.uri().path().starts_with(INGEST_MOUNT_PREFIX) {
            strip_base_prefix(&mut req);
            let connect = self.connect.clone();
            Box::pin(async move {
                let response = connect.oneshot(req).await?;
                // `ConnectRpcBody::Error` is `Infallible`.
                Ok(response.map(|body| body.boxed_unsync()))
            })
        } else {
            let main = self.inner.clone();
            Box::pin(async move {
                let response = main.oneshot(req).await?;
                Ok(response.map(|body| body.boxed_unsync()))
            })
        }
    }
}

/// Strip the leading `/restate` base so the remaining path matches the Connect
/// canonical `/restate.ingestion.IngestionSvc/Ingest`.
fn strip_base_prefix(req: &mut Request<Incoming>) {
    let uri = req.uri();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let stripped = &path_and_query[ingestion::INGEST_BASE.len()..];
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(stripped.parse().expect("valid path-and-query"));
    *req.uri_mut() = Uri::from_parts(parts).expect("valid uri");
}
