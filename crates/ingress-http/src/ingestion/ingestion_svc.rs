// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Experimental Connect (connectrpc) ingestion API.
//!
//! Exposes the bidirectional-streaming `restate.ingestion.IngestionSvc/Ingest`
//! RPC (see `protobuf/ingestion_svc.proto`). This is a **scaffold**: the handler
//! parses the incoming `Settings`/`Record` stream and replies with `Ack`/`Error`,
//! but does not yet write records to the log. Real ingestion (WAL `Envelope` +
//! producer/offset deduplication via `restate_ingestion_client::IngestionClient`)
//! is a follow-up.

use std::sync::Arc;

use connectrpc::{InboundStream, RequestContext, Response, ServiceResult, ServiceStream};
use futures::StreamExt;

use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;

/// Generated protobuf bindings for `restate.ingestion` (see `protobuf/ingestion_svc.proto`).
// Enum variant names mirror the proto (`ErrorKind_UNKNOWN`), which the generator does
// not already allow-list.
// #[allow(clippy::enum_variant_names)]
pub mod proto {
    connectrpc::include_generated!();
}

use proto::restate::ingestion::__buffa::oneof::request::Payload;
use proto::restate::ingestion::__buffa::oneof::response as response_oneof;
use proto::restate::ingestion::{
    IngestionSvc, IngestionSvcExt, Request, Response as IngestResponse, Settings, WindowUpdate,
};

/// Build the Connect tower service for the ingestion API.
pub(crate) fn connect_service<Schemas>(schemas: Live<Schemas>) -> connectrpc::ConnectRpcService
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    let router = Arc::new(IngestionService::new(schemas)).register(connectrpc::Router::new());
    connectrpc::ConnectRpcService::new(router)
}

/// Scaffold implementation of the `IngestionSvc` Connect service.
struct IngestionService<Schemas> {
    schemas: Live<Schemas>,
}

impl<Schemas> IngestionService<Schemas> {
    fn new(schemas: Live<Schemas>) -> Self {
        Self { schemas }
    }
}

impl<Schemas> IngestionSvc for IngestionService<Schemas>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    async fn ingest(
        &self,
        _ctx: RequestContext,
        requests: InboundStream<Request>,
    ) -> ServiceResult<
        ServiceStream<impl connectrpc::Encodable<IngestResponse> + Send + use<Schemas>>,
    > {
        // Snapshot the schema once so the stream owns a `'static` resolver rather
        // than borrowing `&self` across `.await` points.
        let schemas = self.schemas.snapshot();

        let responses = futures::stream::unfold(
            State {
                requests,
                _schemas: schemas,
                settings: None,
            },
            |mut state| async move {
                loop {
                    let message = match state.requests.next().await {
                        None => return None,
                        Some(Ok(message)) => message,
                        Some(Err(err)) => return Some((Err(err), state)),
                    };

                    match message.to_owned_message().payload {
                        // `Settings` establishes the defaults for subsequent records;
                        // its fields are replaced wholesale (not merged) and it emits no ack.
                        Some(Payload::Settings(settings)) => {
                            state.settings = Some(*settings);
                        }
                        Some(Payload::Record(record)) => {
                            return Some((Ok(ack(record.offset)), state));
                        }
                        // Empty request payload: nothing to do.
                        None => {}
                    }
                }
            },
        );

        Response::stream_ok(responses)
    }
}

/// Streaming state carried across the inbound `Request` stream.
struct State<Schemas> {
    requests: InboundStream<Request>,
    _schemas: Arc<Schemas>,
    settings: Option<Settings>,
}

fn ack(offset: u64) -> IngestResponse {
    IngestResponse {
        last_committed: Some(offset),
        response: Some(response_oneof::Response::from(WindowUpdate::default())),
        ..Default::default()
    }
}
