// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use integration::harness::TestBinaryError;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tonic::transport::server::TcpIncoming;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POLL_ATTEMPTS: usize = 200;
const POLL_INTERVAL_MS: u64 = 50;

/// Metadata key the sink's config.toml sets, which Quickwit uses to pick an index.
pub const GRPC_INDEX_HEADER: &str = "qw-otel-traces-index";

#[derive(Default)]
struct Received {
    requests: Vec<ExportTraceServiceRequest>,
    index_headers: Vec<String>,
}

#[derive(Clone, Default)]
struct TraceReceiver {
    received: Arc<Mutex<Received>>,
}

#[tonic::async_trait]
impl TraceService for TraceReceiver {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let index_header = request
            .metadata()
            .get(GRPC_INDEX_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut received = self.received.lock().unwrap();
        received.index_headers.extend(index_header);
        received.requests.push(request.into_inner());
        Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
    }
}

/// An OTLP/gRPC trace receiver served from the test process on an ephemeral
/// loopback port. The connectors runtime dials it like any real collector.
pub struct OtlpGrpcReceiver {
    address: SocketAddr,
    receiver: TraceReceiver,
    _shutdown: oneshot::Sender<()>,
}

impl OtlpGrpcReceiver {
    pub fn start() -> Result<Self, TestBinaryError> {
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback address"))
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "OtlpGrpcReceiver".to_string(),
                message: format!("Failed to bind: {e}"),
            })?;
        let address = incoming
            .local_addr()
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "OtlpGrpcReceiver".to_string(),
                message: format!("Failed to read the bound address: {e}"),
            })?;
        let receiver = TraceReceiver::default();
        let (shutdown, stopped) = oneshot::channel::<()>();
        let server = tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(receiver.clone()))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            });
        tokio::spawn(server);
        Ok(Self {
            address,
            receiver,
            _shutdown: shutdown,
        })
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn index_headers(&self) -> Vec<String> {
        self.receiver.received.lock().unwrap().index_headers.clone()
    }

    pub fn requests(&self) -> Vec<ExportTraceServiceRequest> {
        self.receiver.received.lock().unwrap().requests.clone()
    }

    /// Waits until at least `expected` spans arrived and returns them.
    pub async fn wait_for_spans(&self, expected: usize) -> Result<Vec<Span>, TestBinaryError> {
        for _ in 0..POLL_ATTEMPTS {
            let spans = spans_of(&self.requests());
            if spans.len() >= expected {
                return Ok(spans);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {expected} spans but got {} after {POLL_ATTEMPTS} poll attempts",
                spans_of(&self.requests()).len()
            ),
        })
    }
}

fn spans_of(requests: &[ExportTraceServiceRequest]) -> Vec<Span> {
    requests
        .iter()
        .flat_map(|request| &request.resource_spans)
        .flat_map(|resource| &resource.scope_spans)
        .flat_map(|scope| scope.spans.clone())
        .collect()
}

/// An OTLP/HTTP receiver backed by an in-process WireMock server.
pub struct OtlpHttpReceiver {
    server: MockServer,
}

impl OtlpHttpReceiver {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Self { server }
    }

    pub fn endpoint(&self) -> String {
        self.server.uri()
    }

    /// Waits until at least `expected` spans arrived and returns them.
    pub async fn wait_for_spans(&self, expected: usize) -> Result<Vec<Span>, TestBinaryError> {
        let mut last_count = 0;
        for _ in 0..POLL_ATTEMPTS {
            let requests = self.server.received_requests().await.unwrap_or_default();
            let decoded: Vec<ExportTraceServiceRequest> = requests
                .iter()
                .filter_map(|request| {
                    ExportTraceServiceRequest::decode(request.body.as_slice()).ok()
                })
                .collect();
            let spans = spans_of(&decoded);
            last_count = spans.len();
            if last_count >= expected {
                return Ok(spans);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {expected} spans but got {last_count} after {POLL_ATTEMPTS} poll attempts"
            ),
        })
    }
}
