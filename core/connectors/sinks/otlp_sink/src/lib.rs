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

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use flate2::{Compression, write::GzEncoder};
use http::uri::PathAndQuery;
use iggy_connector_sdk::retry::{
    RetryPolicy, build_retry_client, is_transient_status, retry_async,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    owned_value_into_serde_json, sink_connector,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
use prost::Message;
use reqwest::Url;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tonic::client::Grpc as TonicGrpc;
use tonic::codec::CompressionEncoding;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, error, info, warn};

mod from_json;

sink_connector!(OtlpSink);

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "30s";
const DEFAULT_TIMEOUT: &str = "30s";

const GRPC_TRACES_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
const GRPC_METRICS_PATH: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
const GRPC_LOGS_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

/// Which OTLP signal this sink handles. Must match the Iggy topic content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpSignal {
    Traces,
    Metrics,
    Logs,
}

impl OtlpSignal {
    fn grpc_path(self) -> &'static str {
        match self {
            Self::Traces => GRPC_TRACES_PATH,
            Self::Metrics => GRPC_METRICS_PATH,
            Self::Logs => GRPC_LOGS_PATH,
        }
    }

    fn http_path(self) -> &'static str {
        match self {
            Self::Traces => "/v1/traces",
            Self::Metrics => "/v1/metrics",
            Self::Logs => "/v1/logs",
        }
    }
}

/// How messages are stored in the Iggy topic.
///
/// `Json` (default): messages are JSON produced by the otlp_source JSON path.
/// The sink reconstructs OTLP proto from the JSON before forwarding. The
/// stream's `schema` must be `json`.
///
/// `Proto`: messages are raw prost-encoded OTLP proto bytes (one
/// `Export*ServiceRequest` per message). The sink forwards each one unchanged,
/// without decoding it. The stream's `schema` must be `raw`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageFormat {
    #[default]
    Json,
    Proto,
}

/// Wire transport used to forward OTLP data to the downstream receiver.
///
/// `Grpc` (default): OTLP/gRPC (`opentelemetry.proto.collector.*` service RPCs).
///
/// `Http`: OTLP/HTTP (`POST /v1/{traces,metrics,logs}` with
/// `Content-Type: application/x-protobuf`), for receivers that expose no gRPC port.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    #[default]
    Grpc,
    Http,
}

/// Configuration for the OTLP sink connector, deserialized from `[plugin_config]` in config.toml.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpSinkConfig {
    /// Receiver endpoint with an `http` or `https` scheme, e.g. "http://quickwit:7281".
    /// For `transport = "http"` this is the base URL; `/v1/<signal>` is appended.
    pub endpoint: String,
    pub signal: OtlpSignal,
    #[serde(default)]
    pub transport: Transport,
    #[serde(default)]
    pub format: StorageFormat,
    /// Gzip-compress requests (gRPC message compression or HTTP `Content-Encoding`).
    #[serde(default)]
    pub compression: bool,
    /// Extra headers sent with every export request: gRPC metadata or HTTP headers.
    /// Values are redacted in `Debug` output because they usually carry credentials
    /// (`Authorization`, API keys) or tenant routing, e.g. Quickwit's
    /// `qw-otel-traces-index = "flows3"`.
    #[serde(default)]
    pub headers: HashMap<String, SecretString>,
    /// Log per-batch export counts at `info` instead of `debug` (default: false).
    pub verbose_logging: Option<bool>,
    /// JSON mode only: records per export request (default: 1000).
    pub batch_size: Option<usize>,
    /// Total export attempts including the first (default: 3). 1 disables retries.
    pub max_retries: Option<u32>,
    /// Initial retry delay, e.g. "1s" (default: 1s).
    pub retry_delay: Option<String>,
    /// Maximum retry delay, e.g. "5s" (default: 5s).
    pub retry_max_delay: Option<String>,
    /// Startup connectivity attempts including the first (default: 10).
    pub max_open_retries: Option<u32>,
    /// Maximum delay between startup connectivity attempts (default: 30s).
    pub open_retry_max_delay: Option<String>,
    /// Timeout per export request and per connection attempt (default: 30s).
    #[serde(alias = "request_timeout")]
    pub timeout: Option<String>,
}

type GrpcMetadata = Vec<(MetadataKey<Ascii>, MetadataValue<Ascii>)>;

#[derive(Debug)]
struct GrpcWire {
    channel: Channel,
    metadata: GrpcMetadata,
}

#[derive(Debug)]
struct HttpWire {
    client: ClientWithMiddleware,
    url: String,
    headers: HeaderMap,
}

#[derive(Debug)]
enum Wire {
    Grpc(GrpcWire),
    Http(HttpWire),
}

/// Everything `open()` builds and `consume()` reads.
#[derive(Debug)]
struct Connection {
    wire: Wire,
    retry: RetryPolicy,
    retry_context: String,
    batch_size: usize,
}

/// Cumulative export counters, logged at close().
#[derive(Debug, Default)]
struct Counters {
    messages_sent: AtomicU64,
    /// Messages the sink could not convert: wrong payload variant for the
    /// configured `format`, or a metric without a name.
    messages_dropped: AtomicU64,
    /// Messages whose export request failed after all retries.
    messages_failed: AtomicU64,
    /// Records the receiver rejected through `partial_success`.
    records_rejected: AtomicU64,
    batches_sent: AtomicU64,
    batches_failed: AtomicU64,
}

#[derive(Debug)]
pub struct OtlpSink {
    id: u32,
    config: OtlpSinkConfig,
    verbose: bool,
    connection: Option<Connection>,
    counters: Counters,
}

/// What one `consume()` call achieved.
#[derive(Debug, Default)]
struct Outcome {
    sent: u64,
    dropped: u64,
    failed: u64,
    rejected: u64,
    error: Option<Error>,
}

/// An export attempt failure and whether retrying can help.
#[derive(Debug)]
struct Attempt {
    transient: bool,
    error: Error,
}

impl fmt::Display for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl Attempt {
    fn from_status(status: &tonic::Status) -> Self {
        use tonic::Code;
        // The retryable set of the OTLP specification, plus Unknown, which tonic
        // reports for transport failures, and ResourceExhausted, the gRPC
        // counterpart of HTTP 429.
        let reason = format!("OTLP gRPC export: {status}");
        match status.code() {
            Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown => Self {
                transient: true,
                error: Error::Connection(reason),
            },
            Code::ResourceExhausted | Code::Aborted | Code::OutOfRange | Code::DataLoss => Self {
                transient: true,
                error: Error::CannotStoreData(reason),
            },
            _ => Self {
                transient: false,
                error: Error::PermanentHttpError(reason),
            },
        }
    }
}

/// The part of an `Export*Response` the sink acts on.
#[derive(Debug, PartialEq, Eq)]
struct PartialSuccess {
    rejected: u64,
    message: String,
}

impl OtlpSink {
    pub fn new(id: u32, config: OtlpSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        Self {
            id,
            config,
            verbose,
            connection: None,
            counters: Counters::default(),
        }
    }

    fn connection(&self) -> Result<&Connection, Error> {
        self.connection
            .as_ref()
            .ok_or_else(|| Error::InitError("OTLP sink is not open".into()))
    }

    async fn connect(&self) -> Result<Connection, Error> {
        let retry_delay = parse_duration(
            self.config.retry_delay.as_deref(),
            DEFAULT_RETRY_DELAY,
            "retry_delay",
        )?;
        let retry_max_delay = parse_duration(
            self.config.retry_max_delay.as_deref(),
            DEFAULT_RETRY_MAX_DELAY,
            "retry_max_delay",
        )?;
        let open_retry_max_delay = parse_duration(
            self.config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
            "open_retry_max_delay",
        )?;
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT, "timeout")?;
        let max_retries = self
            .config
            .max_retries
            .unwrap_or(DEFAULT_MAX_RETRIES)
            .max(1);
        let endpoint = parse_endpoint(&self.config.endpoint)?;
        let open_policy = RetryPolicy {
            max_attempts: self
                .config
                .max_open_retries
                .unwrap_or(DEFAULT_MAX_OPEN_RETRIES)
                .max(1),
            base_delay: retry_delay,
            max_delay: open_retry_max_delay,
        };

        let wire = match self.config.transport {
            Transport::Grpc => {
                let metadata = self.grpc_metadata()?;
                let channel = self.connect_grpc(&endpoint, timeout, open_policy).await?;
                Wire::Grpc(GrpcWire { channel, metadata })
            }
            Transport::Http => {
                let headers = self.http_headers()?;
                let raw_client = reqwest::Client::builder()
                    .timeout(timeout)
                    // A redirected POST would be replayed as a GET, silently dropping the data.
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|e| Error::InitError(format!("HTTP client: {e}")))?;
                self.probe_http(&endpoint, timeout, open_policy).await?;
                let base = self.config.endpoint.trim_end_matches('/');
                Wire::Http(HttpWire {
                    client: build_retry_client(
                        raw_client,
                        max_retries,
                        retry_delay,
                        retry_max_delay,
                        "OTLP sink",
                    ),
                    url: format!("{base}{}", self.config.signal.http_path()),
                    headers,
                })
            }
        };

        Ok(Connection {
            wire,
            retry: RetryPolicy {
                max_attempts: max_retries,
                base_delay: retry_delay,
                max_delay: retry_max_delay,
            },
            retry_context: format!("OTLP sink ID {} export", self.id),
            batch_size: self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1),
        })
    }

    fn grpc_metadata(&self) -> Result<GrpcMetadata, Error> {
        let mut metadata = Vec::with_capacity(self.config.headers.len());
        for (name, value) in &self.config.headers {
            let key = MetadataKey::from_bytes(name.as_bytes())
                .map_err(|e| Error::InvalidConfigValue(format!("header key '{name}': {e}")))?;
            let mut value = MetadataValue::try_from(value.expose_secret()).map_err(|e| {
                Error::InvalidConfigValue(format!("header value for '{name}': {e}"))
            })?;
            value.set_sensitive(true);
            metadata.push((key, value));
        }
        Ok(metadata)
    }

    fn http_headers(&self) -> Result<HeaderMap, Error> {
        let mut headers = HeaderMap::with_capacity(self.config.headers.len() + 2);
        for (name, value) in &self.config.headers {
            let key = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| Error::InvalidConfigValue(format!("header key '{name}': {e}")))?;
            let mut value = HeaderValue::from_str(value.expose_secret()).map_err(|e| {
                Error::InvalidConfigValue(format!("header value for '{name}': {e}"))
            })?;
            value.set_sensitive(true);
            headers.insert(key, value);
        }
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        if self.config.compression {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }
        Ok(headers)
    }

    async fn connect_grpc(
        &self,
        endpoint: &Url,
        timeout: Duration,
        policy: RetryPolicy,
    ) -> Result<Channel, Error> {
        let context = format!("OTLP sink ID {} startup connectivity", self.id);
        retry_async(
            policy,
            &context,
            |_| true,
            || async {
                let mut builder = Endpoint::from_shared(endpoint.to_string())
                    .map_err(|e| Error::InvalidConfigValue(format!("endpoint: {e}")))?
                    .timeout(timeout)
                    .connect_timeout(timeout);
                if endpoint.scheme() == "https" {
                    builder = builder
                        .tls_config(ClientTlsConfig::new().with_enabled_roots())
                        .map_err(|e| Error::InvalidConfigValue(format!("tls: {e}")))?;
                }
                builder
                    .connect()
                    .await
                    .map_err(|e| Error::Connection(format!("OTLP endpoint {endpoint}: {e}")))
            },
        )
        .await
        .map_err(|failure| {
            error!("{context} {failure}");
            as_init_error(failure.into_error())
        })
    }

    /// OTLP/HTTP defines no health endpoint, so readiness is a TCP connect.
    async fn probe_http(
        &self,
        endpoint: &Url,
        timeout: Duration,
        policy: RetryPolicy,
    ) -> Result<(), Error> {
        let context = format!("OTLP sink ID {} startup connectivity", self.id);
        let address = socket_address(endpoint).await?;
        retry_async(
            policy,
            &context,
            |_| true,
            || async {
                match tokio::time::timeout(timeout, TcpStream::connect(address)).await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(Error::Connection(format!("OTLP endpoint {endpoint}: {e}"))),
                    Err(_) => Err(Error::Connection(format!(
                        "OTLP endpoint {endpoint}: connect timed out"
                    ))),
                }
            },
        )
        .await
        .map_err(|failure| {
            error!("{context} {failure}");
            as_init_error(failure.into_error())
        })
    }

    async fn deliver(&self, connection: &Connection, messages: Vec<ConsumedMessage>) -> Outcome {
        let mut outcome = Outcome::default();
        match self.config.format {
            StorageFormat::Json => {
                self.deliver_json(connection, messages, &mut outcome).await;
            }
            StorageFormat::Proto => {
                self.deliver_proto(connection, messages, &mut outcome).await;
            }
        }
        outcome
    }

    async fn deliver_json(
        &self,
        connection: &Connection,
        messages: Vec<ConsumedMessage>,
        outcome: &mut Outcome,
    ) {
        let mut documents = Vec::with_capacity(messages.len());
        for message in messages {
            let offset = message.offset;
            match message.payload.into_json_document() {
                Payload::Json(document) => documents.push(owned_value_into_serde_json(document)),
                other => {
                    outcome.dropped += 1;
                    warn!(
                        "OTLP sink connector ID: {} (json mode): expected a JSON payload at offset {offset}, got schema: {}",
                        self.id,
                        other.schema()
                    );
                }
            }
        }

        for chunk in documents.chunks(connection.batch_size) {
            let (body, converted) = self.encode_chunk(chunk);
            outcome.dropped += (chunk.len() - converted) as u64;
            if converted == 0 {
                continue;
            }
            match self.export(connection, body).await {
                Ok(rejected) => {
                    let rejected = rejected.min(converted as u64);
                    outcome.rejected += rejected;
                    outcome.sent += converted as u64 - rejected;
                }
                Err(e) => {
                    outcome.failed += converted as u64;
                    outcome.error = Some(e);
                }
            }
        }
    }

    /// One request per message: a failed message does not lose the ones after it.
    async fn deliver_proto(
        &self,
        connection: &Connection,
        messages: Vec<ConsumedMessage>,
        outcome: &mut Outcome,
    ) {
        for message in messages {
            let offset = message.offset;
            let Payload::Raw(bytes) = message.payload else {
                outcome.dropped += 1;
                warn!(
                    "OTLP sink connector ID: {} (proto mode): expected a raw payload at offset {offset}, got schema: {}",
                    self.id,
                    message.payload.schema()
                );
                continue;
            };
            match self.export(connection, Bytes::from(bytes)).await {
                Ok(rejected) => {
                    outcome.rejected += rejected;
                    outcome.sent += 1;
                }
                Err(e) => {
                    outcome.failed += 1;
                    outcome.error = Some(e);
                }
            }
        }
    }

    /// Returns the encoded request and how many of the documents it holds.
    fn encode_chunk(&self, documents: &[serde_json::Value]) -> (Bytes, usize) {
        match self.config.signal {
            OtlpSignal::Traces => {
                let request = from_json::traces_from_json(documents);
                let converted = request
                    .resource_spans
                    .iter()
                    .flat_map(|resource| &resource.scope_spans)
                    .map(|scope| scope.spans.len())
                    .sum();
                (Bytes::from(request.encode_to_vec()), converted)
            }
            OtlpSignal::Logs => {
                let request = from_json::logs_from_json(documents);
                (Bytes::from(request.encode_to_vec()), documents.len())
            }
            OtlpSignal::Metrics => {
                let request = from_json::metrics_from_json(documents);
                let converted = request
                    .resource_metrics
                    .iter()
                    .flat_map(|resource| &resource.scope_metrics)
                    .map(|scope| scope.metrics.len())
                    .sum();
                (Bytes::from(request.encode_to_vec()), converted)
            }
        }
    }

    /// Sends one encoded `Export*ServiceRequest` and returns how many records
    /// the receiver rejected through `partial_success`.
    async fn export(&self, connection: &Connection, body: Bytes) -> Result<u64, Error> {
        let response = match &connection.wire {
            Wire::Grpc(grpc) => self.export_grpc(connection, grpc, body).await?,
            Wire::Http(http) => self.export_http(http, body).await?,
        };
        match decode_partial_success(self.config.signal, &response) {
            Ok(Some(partial)) if partial.rejected > 0 => {
                warn!(
                    "OTLP sink connector ID: {} receiver rejected {} records: {}",
                    self.id, partial.rejected, partial.message
                );
                Ok(partial.rejected)
            }
            Ok(Some(partial)) => {
                warn!(
                    "OTLP sink connector ID: {} receiver accepted the request with a warning: {}",
                    self.id, partial.message
                );
                Ok(0)
            }
            Ok(None) => Ok(0),
            Err(e) => {
                warn!(
                    "OTLP sink connector ID: {} could not decode the export response: {e}",
                    self.id
                );
                Ok(0)
            }
        }
    }

    async fn export_grpc(
        &self,
        connection: &Connection,
        grpc: &GrpcWire,
        body: Bytes,
    ) -> Result<Bytes, Error> {
        let path = PathAndQuery::from_static(self.config.signal.grpc_path());
        retry_async(
            connection.retry,
            &connection.retry_context,
            |attempt: &Attempt| attempt.transient,
            || async {
                let mut client = TonicGrpc::new(grpc.channel.clone());
                if self.config.compression {
                    client = client
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip);
                }
                // tower::Buffer requires poll_ready before every call.
                client.ready().await.map_err(|e| {
                    Attempt::from_status(&tonic::Status::unavailable(format!(
                        "channel not ready: {e}"
                    )))
                })?;
                let mut request = tonic::Request::new(body.clone());
                for (key, value) in &grpc.metadata {
                    request.metadata_mut().insert(key.clone(), value.clone());
                }
                client
                    .unary(request, path.clone(), RawCodec)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|status| Attempt::from_status(&status))
            },
        )
        .await
        .map_err(|failure| {
            error!(
                "OTLP sink connector ID: {} gRPC export failed: {failure}",
                self.id
            );
            failure.into_error().error
        })
    }

    async fn export_http(&self, http: &HttpWire, body: Bytes) -> Result<Bytes, Error> {
        let body = if self.config.compression {
            Bytes::from(gzip(&body)?)
        } else {
            body
        };
        // The retry middleware retries 429, 5xx and network errors, honoring
        // Retry-After, and hands back the final response.
        let response = http
            .client
            .post(&http.url)
            .headers(http.headers.clone())
            .body(body)
            .send()
            .await
            .map_err(|e| Error::HttpRequestFailed(format!("OTLP HTTP {}: {e}", http.url)))?;
        let status = response.status();
        let payload = response.bytes().await.map_err(|e| {
            Error::HttpRequestFailed(format!("OTLP HTTP {} response body: {e}", http.url))
        })?;
        if status.is_success() {
            return Ok(payload);
        }
        let reason = format!(
            "OTLP HTTP {} returned {status}: {}",
            http.url,
            String::from_utf8_lossy(&payload)
        );
        error!("OTLP sink connector ID: {} {reason}", self.id);
        if is_transient_status(status) {
            Err(Error::HttpRequestFailed(reason))
        } else {
            Err(Error::PermanentHttpError(reason))
        }
    }

    fn finish(&self, total: usize, outcome: Outcome) -> Result<(), Error> {
        let Outcome {
            sent,
            dropped,
            failed,
            rejected,
            error,
        } = outcome;
        let counters = &self.counters;
        counters.messages_sent.fetch_add(sent, Ordering::Relaxed);
        counters
            .messages_dropped
            .fetch_add(dropped, Ordering::Relaxed);
        counters
            .messages_failed
            .fetch_add(failed, Ordering::Relaxed);
        counters
            .records_rejected
            .fetch_add(rejected, Ordering::Relaxed);

        if error.is_none() && dropped == 0 && rejected == 0 {
            counters.batches_sent.fetch_add(1, Ordering::Relaxed);
            if self.verbose {
                info!(
                    "OTLP sink connector ID: {} exported {sent} messages",
                    self.id
                );
            } else {
                debug!(
                    "OTLP sink connector ID: {} exported {sent} messages",
                    self.id
                );
            }
            return Ok(());
        }

        counters.batches_failed.fetch_add(1, Ordering::Relaxed);
        error!(
            "OTLP sink connector ID: {} exported {sent} of {total} messages: {failed} failed, {dropped} dropped, {rejected} records rejected by the receiver. The runtime does not redeliver them.",
            self.id
        );
        Err(error.unwrap_or_else(|| {
            if rejected > 0 {
                Error::PermanentHttpError(format!(
                    "OTLP receiver rejected {rejected} records (partial_success)"
                ))
            } else {
                Error::SchemaMismatch(format!(
                    "{dropped} of {total} messages are not valid for format {:?}",
                    self.config.format
                ))
            }
        }))
    }
}

#[async_trait]
impl Sink for OtlpSink {
    async fn open(&mut self) -> Result<(), Error> {
        match self.connect().await {
            Ok(connection) => self.connection = Some(connection),
            Err(e) => {
                self.connection = None;
                error!("Failed to open OTLP sink connector ID: {}: {e}", self.id);
                return Err(e);
            }
        }
        info!(
            "Opened OTLP sink connector ID: {}, signal: {:?}, transport: {:?}, format: {:?}, endpoint: {}",
            self.id,
            self.config.signal,
            self.config.transport,
            self.config.format,
            redacted_endpoint(&self.config.endpoint)
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total = messages.len();
        if self.verbose {
            info!(
                "OTLP sink connector ID: {} received {total} messages, schema: {}",
                self.id, messages_metadata.schema
            );
        } else {
            debug!(
                "OTLP sink connector ID: {} received {total} messages, schema: {}",
                self.id, messages_metadata.schema
            );
        }
        // The runtime also calls with no messages when every message in the batch was dropped.
        if total == 0 {
            return Ok(());
        }
        let connection = self.connection()?;
        let outcome = self.deliver(connection, messages).await;
        self.finish(total, outcome)
    }

    async fn close(&mut self) -> Result<(), Error> {
        let _ = self.connection.take();
        info!(
            "Closed OTLP sink connector ID: {}. Counters: messages_sent={}, messages_dropped={}, messages_failed={}, records_rejected={}, batches_sent={}, batches_failed={}",
            self.id,
            self.counters.messages_sent.load(Ordering::Relaxed),
            self.counters.messages_dropped.load(Ordering::Relaxed),
            self.counters.messages_failed.load(Ordering::Relaxed),
            self.counters.records_rejected.load(Ordering::Relaxed),
            self.counters.batches_sent.load(Ordering::Relaxed),
            self.counters.batches_failed.load(Ordering::Relaxed),
        );
        Ok(())
    }
}

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let duration = humantime::parse_duration(value.unwrap_or(default))
        .map_err(|e| Error::InvalidConfigValue(format!("{field}: {e}")))?;
    if duration.is_zero() {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(duration)
}

fn parse_endpoint(endpoint: &str) -> Result<Url, Error> {
    let url =
        Url::parse(endpoint).map_err(|e| Error::InvalidConfigValue(format!("endpoint: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") || !url.has_host() {
        return Err(Error::InvalidConfigValue(
            "endpoint must be an http or https URL with a host".into(),
        ));
    }
    Ok(url)
}

async fn socket_address(url: &Url) -> Result<SocketAddr, Error> {
    let host = url
        .host_str()
        .ok_or_else(|| Error::InvalidConfigValue("endpoint has no host".into()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| Error::InvalidConfigValue("endpoint has no port".into()))?;
    // `host_str` keeps the brackets of an IPv6 literal, which the resolver rejects.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| Error::InitError(format!("resolving OTLP endpoint {host}: {e}")))?
        .next()
        .ok_or_else(|| Error::InitError(format!("OTLP endpoint {host} has no address")))
}

/// Keeps credentials embedded in the endpoint out of the logs.
fn redacted_endpoint(endpoint: &str) -> String {
    match Url::parse(endpoint) {
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.to_string()
        }
        Err(_) => "<invalid>".to_string(),
    }
}

/// `open()` failures reach the runtime as one line, so they carry the init error class.
fn as_init_error(error: Error) -> Error {
    match error {
        Error::InvalidConfigValue(_) | Error::InitError(_) => error,
        other => Error::InitError(other.to_string()),
    }
}

fn gzip(data: &[u8]) -> Result<Vec<u8>, Error> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len() / 2), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| Error::Serialization(format!("gzip encode: {e}")))?;
    encoder
        .finish()
        .map_err(|e| Error::Serialization(format!("gzip finish: {e}")))
}

fn decode_partial_success(
    signal: OtlpSignal,
    response: &[u8],
) -> Result<Option<PartialSuccess>, prost::DecodeError> {
    if response.is_empty() {
        return Ok(None);
    }
    let partial = match signal {
        OtlpSignal::Traces => ExportTraceServiceResponse::decode(response)?
            .partial_success
            .map(|p| (p.rejected_spans, p.error_message)),
        OtlpSignal::Metrics => ExportMetricsServiceResponse::decode(response)?
            .partial_success
            .map(|p| (p.rejected_data_points, p.error_message)),
        OtlpSignal::Logs => ExportLogsServiceResponse::decode(response)?
            .partial_success
            .map(|p| (p.rejected_log_records, p.error_message)),
    };
    Ok(partial
        .filter(|(rejected, message)| *rejected > 0 || !message.is_empty())
        .map(|(rejected, message)| PartialSuccess {
            rejected: u64::try_from(rejected).unwrap_or(0),
            message,
        }))
}

// Passthrough codec: encodes raw Bytes as-is into the gRPC frame and hands the
// response body back undecoded so the caller can read partial_success.
#[derive(Default)]
struct RawCodec;
struct RawEncoder;
struct RawDecoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Self::Error> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> RawEncoder {
        RawEncoder
    }

    fn decoder(&mut self) -> RawDecoder {
        RawDecoder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
        LogsService, LogsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceRequest,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
        MetricsService, MetricsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceRequest,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
        TraceService, TraceServiceServer,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTracePartialSuccess, ExportTraceServiceRequest,
    };
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    use tonic::transport::server::TcpIncoming;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// What the mock receiver answers to the next export call.
    #[derive(Debug, Clone)]
    enum Reply {
        Accept,
        Reject {
            rejected: i64,
            message: &'static str,
        },
        Fail(tonic::Code),
    }

    #[derive(Debug, Default)]
    struct Received {
        calls: usize,
        traces: Vec<ExportTraceServiceRequest>,
        metrics: Vec<ExportMetricsServiceRequest>,
        logs: Vec<ExportLogsServiceRequest>,
        index_header: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct MockReceiver {
        received: Arc<Mutex<Received>>,
        replies: Arc<Mutex<VecDeque<Reply>>>,
    }

    impl MockReceiver {
        fn with_replies(replies: Vec<Reply>) -> Self {
            Self {
                received: Arc::default(),
                replies: Arc::new(Mutex::new(replies.into())),
            }
        }

        fn next_reply(&self, metadata: &tonic::metadata::MetadataMap) -> Reply {
            let mut received = self.received.lock().unwrap();
            received.calls += 1;
            if let Some(value) = metadata.get("qw-otel-traces-index") {
                received
                    .index_header
                    .push(value.to_str().unwrap().to_string());
            }
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Reply::Accept)
        }

        fn calls(&self) -> usize {
            self.received.lock().unwrap().calls
        }
    }

    #[tonic::async_trait]
    impl TraceService for MockReceiver {
        async fn export(
            &self,
            request: tonic::Request<ExportTraceServiceRequest>,
        ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
            let reply = self.next_reply(request.metadata());
            match reply {
                Reply::Fail(code) => Err(tonic::Status::new(code, "mock failure")),
                Reply::Accept => {
                    self.received
                        .lock()
                        .unwrap()
                        .traces
                        .push(request.into_inner());
                    Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
                }
                Reply::Reject { rejected, message } => {
                    self.received
                        .lock()
                        .unwrap()
                        .traces
                        .push(request.into_inner());
                    Ok(tonic::Response::new(ExportTraceServiceResponse {
                        partial_success: Some(ExportTracePartialSuccess {
                            rejected_spans: rejected,
                            error_message: message.to_string(),
                        }),
                    }))
                }
            }
        }
    }

    #[tonic::async_trait]
    impl MetricsService for MockReceiver {
        async fn export(
            &self,
            request: tonic::Request<ExportMetricsServiceRequest>,
        ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
            let reply = self.next_reply(request.metadata());
            match reply {
                Reply::Fail(code) => Err(tonic::Status::new(code, "mock failure")),
                Reply::Accept => {
                    self.received
                        .lock()
                        .unwrap()
                        .metrics
                        .push(request.into_inner());
                    Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
                }
                Reply::Reject { rejected, message } => {
                    self.received
                        .lock()
                        .unwrap()
                        .metrics
                        .push(request.into_inner());
                    Ok(tonic::Response::new(ExportMetricsServiceResponse {
                        partial_success: Some(ExportMetricsPartialSuccess {
                            rejected_data_points: rejected,
                            error_message: message.to_string(),
                        }),
                    }))
                }
            }
        }
    }

    #[tonic::async_trait]
    impl LogsService for MockReceiver {
        async fn export(
            &self,
            request: tonic::Request<ExportLogsServiceRequest>,
        ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
            let reply = self.next_reply(request.metadata());
            match reply {
                Reply::Fail(code) => Err(tonic::Status::new(code, "mock failure")),
                Reply::Accept => {
                    self.received
                        .lock()
                        .unwrap()
                        .logs
                        .push(request.into_inner());
                    Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
                }
                Reply::Reject { rejected, message } => {
                    self.received
                        .lock()
                        .unwrap()
                        .logs
                        .push(request.into_inner());
                    Ok(tonic::Response::new(ExportLogsServiceResponse {
                        partial_success: Some(ExportLogsPartialSuccess {
                            rejected_log_records: rejected,
                            error_message: message.to_string(),
                        }),
                    }))
                }
            }
        }
    }

    /// Serves the mock on an ephemeral loopback port until the returned sender is dropped.
    fn start_receiver(mock: &MockReceiver) -> (String, oneshot::Sender<()>) {
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tonic::transport::Server::builder()
            .add_service(
                TraceServiceServer::new(mock.clone()).accept_compressed(CompressionEncoding::Gzip),
            )
            .add_service(
                MetricsServiceServer::new(mock.clone())
                    .accept_compressed(CompressionEncoding::Gzip),
            )
            .add_service(
                LogsServiceServer::new(mock.clone()).accept_compressed(CompressionEncoding::Gzip),
            )
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            });
        tokio::spawn(server);
        (format!("http://{address}"), stop)
    }

    fn test_config(endpoint: &str, signal: OtlpSignal) -> OtlpSinkConfig {
        OtlpSinkConfig {
            endpoint: endpoint.to_string(),
            signal,
            transport: Transport::Grpc,
            format: StorageFormat::Json,
            compression: false,
            headers: HashMap::new(),
            verbose_logging: None,
            batch_size: None,
            max_retries: Some(3),
            retry_delay: Some("1ms".to_string()),
            retry_max_delay: Some("5ms".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("5ms".to_string()),
            timeout: Some("5s".to_string()),
        }
    }

    async fn open_sink(config: OtlpSinkConfig) -> OtlpSink {
        let mut sink = OtlpSink::new(1, config);
        sink.open().await.expect("sink should open");
        sink
    }

    fn json_message(offset: u64, value: &str) -> ConsumedMessage {
        let mut bytes = value.as_bytes().to_vec();
        message(
            offset,
            Payload::Json(simd_json::to_owned_value(&mut bytes).unwrap()),
        )
    }

    fn message(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: u128::from(offset),
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    fn span_json(name: &str) -> String {
        format!(
            r#"{{"trace_id":"0102030405060708090a0b0c0d0e0f10","span_id":"0102030405060708","name":"{name}","resource":{{"service.name":"svc"}}}}"#
        )
    }

    fn trace_request(span_names: &[&str]) -> Bytes {
        let messages: Vec<_> = span_names
            .iter()
            .map(|name| serde_json::from_str(&span_json(name)).unwrap())
            .collect();
        Bytes::from(from_json::traces_from_json(&messages).encode_to_vec())
    }

    async fn consume(sink: &OtlpSink, messages: Vec<ConsumedMessage>) -> Result<(), Error> {
        let topic = TopicMetadata {
            stream: "otel".into(),
            topic: "traces".into(),
        };
        let metadata = MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: Schema::Json,
        };
        sink.consume(&topic, metadata, messages).await
    }

    fn counter(value: &AtomicU64) -> u64 {
        value.load(Ordering::Relaxed)
    }

    #[test]
    fn given_minimal_config_should_apply_defaults() {
        let config: OtlpSinkConfig =
            serde_json::from_str(r#"{"endpoint":"http://qw:7281","signal":"traces"}"#).unwrap();
        assert_eq!(config.transport, Transport::Grpc);
        assert_eq!(config.format, StorageFormat::Json);
        assert!(!config.compression);
        assert!(config.headers.is_empty());
        assert!(config.timeout.is_none());
    }

    #[test]
    fn given_unknown_config_key_should_be_rejected() {
        let result = serde_json::from_str::<OtlpSinkConfig>(
            r#"{"endpoint":"http://qw:7281","signal":"traces","poll_interval":"1s"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn given_request_timeout_alias_should_populate_timeout() {
        let config: OtlpSinkConfig = serde_json::from_str(
            r#"{"endpoint":"http://qw:7281","signal":"logs","request_timeout":"7s"}"#,
        )
        .unwrap();
        assert_eq!(config.timeout.as_deref(), Some("7s"));
    }

    #[test]
    fn given_headers_when_formatted_with_debug_should_redact_values() {
        let config: OtlpSinkConfig = serde_json::from_str(
            r#"{"endpoint":"http://qw:7281","signal":"logs","headers":{"authorization":"Bearer hunter2"}}"#,
        )
        .unwrap();
        let debug = format!("{config:?}");
        assert!(debug.contains("authorization"));
        assert!(!debug.contains("hunter2"));
    }

    #[test]
    fn given_status_codes_should_classify_transient_and_permanent() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::ResourceExhausted,
            tonic::Code::Aborted,
        ] {
            assert!(
                Attempt::from_status(&tonic::Status::new(code, "x")).transient,
                "{code:?} should be retried"
            );
        }
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::Unimplemented,
        ] {
            let attempt = Attempt::from_status(&tonic::Status::new(code, "x"));
            assert!(!attempt.transient, "{code:?} should not be retried");
            assert!(matches!(attempt.error, Error::PermanentHttpError(_)));
        }
    }

    #[test]
    fn given_response_with_rejections_should_decode_partial_success() {
        let response = ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: 3,
                error_message: "bad ids".into(),
            }),
        }
        .encode_to_vec();
        let partial = decode_partial_success(OtlpSignal::Traces, &response)
            .unwrap()
            .unwrap();
        assert_eq!(
            partial,
            PartialSuccess {
                rejected: 3,
                message: "bad ids".into()
            }
        );
        assert_eq!(
            decode_partial_success(OtlpSignal::Traces, &[]).unwrap(),
            None
        );
        assert!(decode_partial_success(OtlpSignal::Logs, &[0xff, 0xff]).is_err());
    }

    #[tokio::test]
    async fn given_invalid_config_when_opened_should_fail_without_connecting() {
        let bad_endpoint = test_config("ftp://qw:7281", OtlpSignal::Traces);
        let mut bad_duration = test_config("http://127.0.0.1:1", OtlpSignal::Traces);
        bad_duration.retry_delay = Some("soon".into());
        let mut zero_timeout = test_config("http://127.0.0.1:1", OtlpSignal::Traces);
        zero_timeout.timeout = Some("0s".into());
        let mut bad_header = test_config("http://127.0.0.1:1", OtlpSignal::Traces);
        bad_header
            .headers
            .insert("bad header".into(), SecretString::from("v"));
        let mut bad_header_value = test_config("http://127.0.0.1:1", OtlpSignal::Traces);
        bad_header_value.transport = Transport::Http;
        bad_header_value
            .headers
            .insert("x-ok".into(), SecretString::from("line\nbreak"));

        for config in [
            bad_endpoint,
            bad_duration,
            zero_timeout,
            bad_header,
            bad_header_value,
        ] {
            let mut sink = OtlpSink::new(1, config);
            let error = sink.open().await.expect_err("config should be refused");
            assert!(
                matches!(error, Error::InvalidConfigValue(_)),
                "unexpected error: {error}"
            );
            assert!(sink.connection.is_none());
        }
    }

    #[tokio::test]
    async fn given_unreachable_endpoint_when_opened_should_return_init_error() {
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = free.local_addr().unwrap();
        drop(free);
        for transport in [Transport::Grpc, Transport::Http] {
            let mut config = test_config(&format!("http://{address}"), OtlpSignal::Traces);
            config.transport = transport;
            config.timeout = Some("1s".into());
            let mut sink = OtlpSink::new(1, config);
            let error = sink.open().await.expect_err("nothing listens there");
            assert!(
                matches!(error, Error::InitError(_)),
                "{transport:?}: unexpected error {error}"
            );
        }
    }

    #[tokio::test]
    async fn given_https_endpoint_when_nothing_listens_should_fail_on_connect_not_on_tls_setup() {
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = free.local_addr().unwrap();
        drop(free);
        let mut config = test_config(&format!("https://{address}"), OtlpSignal::Traces);
        config.timeout = Some("1s".into());
        let mut sink = OtlpSink::new(1, config);
        let error = sink.open().await.expect_err("nothing listens there");
        assert!(matches!(error, Error::InitError(_)), "{error}");
    }

    #[tokio::test]
    async fn given_consume_before_open_should_return_init_error() {
        let sink = OtlpSink::new(1, test_config("http://127.0.0.1:1", OtlpSignal::Traces));
        let error = consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .expect_err("sink is not open");
        assert!(matches!(error, Error::InitError(_)));
    }

    #[tokio::test]
    async fn given_no_messages_when_consumed_should_not_call_the_receiver() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;
        consume(&sink, vec![]).await.unwrap();
        assert_eq!(mock.calls(), 0);
        assert_eq!(counter(&sink.counters.batches_sent), 0);
    }

    #[tokio::test]
    async fn given_json_spans_when_consumed_should_export_one_request_with_headers() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let mut config = test_config(&endpoint, OtlpSignal::Traces);
        config
            .headers
            .insert("qw-otel-traces-index".into(), SecretString::from("flows3"));
        let sink = open_sink(config).await;

        consume(
            &sink,
            vec![
                json_message(0, &span_json("a")),
                json_message(1, &span_json("b")),
            ],
        )
        .await
        .unwrap();

        let received = mock.received.lock().unwrap();
        assert_eq!(received.calls, 1);
        let spans: usize = received.traces[0]
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .map(|scope| scope.spans.len())
            .sum();
        assert_eq!(spans, 2);
        assert_eq!(received.index_header, vec!["flows3".to_string()]);
        assert_eq!(counter(&sink.counters.messages_sent), 2);
        assert_eq!(counter(&sink.counters.batches_sent), 1);
    }

    #[tokio::test]
    async fn given_batch_size_when_consumed_should_split_requests() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let mut config = test_config(&endpoint, OtlpSignal::Traces);
        config.batch_size = Some(2);
        let sink = open_sink(config).await;
        let messages = (0..5)
            .map(|offset| json_message(offset, &span_json("s")))
            .collect();
        consume(&sink, messages).await.unwrap();
        assert_eq!(mock.calls(), 3);
        assert_eq!(counter(&sink.counters.messages_sent), 5);
    }

    #[tokio::test]
    async fn given_json_logs_and_metrics_when_consumed_should_reach_matching_services() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);

        let logs = open_sink(test_config(&endpoint, OtlpSignal::Logs)).await;
        consume(
            &logs,
            vec![json_message(0, r#"{"body":"hello","severity":"info"}"#)],
        )
        .await
        .unwrap();
        let metrics = open_sink(test_config(&endpoint, OtlpSignal::Metrics)).await;
        consume(
            &metrics,
            vec![json_message(
                0,
                r#"{"name":"cpu","value":1.5,"type":"gauge"}"#,
            )],
        )
        .await
        .unwrap();

        let received = mock.received.lock().unwrap();
        assert_eq!(received.logs.len(), 1);
        assert_eq!(received.metrics.len(), 1);
        assert!(received.traces.is_empty());
    }

    #[tokio::test]
    async fn given_metric_without_name_when_consumed_should_export_rest_and_fail_batch() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Metrics)).await;

        let error = consume(
            &sink,
            vec![
                json_message(0, r#"{"name":"cpu","value":1.0}"#),
                json_message(1, r#"{"value":2.0}"#),
            ],
        )
        .await
        .expect_err("a dropped message must fail the batch");

        assert!(matches!(error, Error::SchemaMismatch(_)), "{error}");
        assert_eq!(mock.calls(), 1);
        assert_eq!(counter(&sink.counters.messages_sent), 1);
        assert_eq!(counter(&sink.counters.messages_dropped), 1);
        assert_eq!(counter(&sink.counters.batches_failed), 1);
    }

    #[tokio::test]
    async fn given_span_with_malformed_ids_when_consumed_should_export_rest_and_fail_batch() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;

        let error = consume(
            &sink,
            vec![
                json_message(0, &span_json("ok")),
                json_message(1, r#"{"name":"bad","trace_id":"0102","span_id":"01"}"#),
            ],
        )
        .await
        .expect_err("a span the receiver would reject must fail the batch");

        assert!(matches!(error, Error::SchemaMismatch(_)), "{error}");
        assert_eq!(counter(&sink.counters.messages_sent), 1);
        assert_eq!(counter(&sink.counters.messages_dropped), 1);
    }

    #[tokio::test]
    async fn given_non_json_payload_in_json_mode_when_consumed_should_export_rest_and_fail_batch() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;

        let error = consume(
            &sink,
            vec![
                json_message(0, &span_json("ok")),
                message(1, Payload::Raw(vec![1, 2, 3])),
                message(2, Payload::Text("plain".into())),
            ],
        )
        .await
        .expect_err("dropped messages must fail the batch");

        assert!(matches!(error, Error::SchemaMismatch(_)), "{error}");
        assert_eq!(counter(&sink.counters.messages_sent), 1);
        assert_eq!(counter(&sink.counters.messages_dropped), 2);
    }

    #[tokio::test]
    async fn given_proto_text_holding_json_in_json_mode_when_consumed_should_export_it() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;
        consume(&sink, vec![message(0, Payload::Proto(span_json("p")))])
            .await
            .unwrap();
        assert_eq!(counter(&sink.counters.messages_sent), 1);
    }

    #[tokio::test]
    async fn given_every_message_undecodable_when_consumed_should_fail_without_exporting() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;

        let error = consume(&sink, vec![message(0, Payload::Raw(vec![1]))])
            .await
            .expect_err("nothing exportable must be an error");

        assert!(matches!(error, Error::SchemaMismatch(_)), "{error}");
        assert_eq!(mock.calls(), 0);
        assert_eq!(counter(&sink.counters.messages_sent), 0);
    }

    #[tokio::test]
    async fn given_proto_mode_with_wrong_payload_when_consumed_should_export_rest_and_fail_batch() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let mut config = test_config(&endpoint, OtlpSignal::Traces);
        config.format = StorageFormat::Proto;
        let sink = open_sink(config).await;

        let error = consume(
            &sink,
            vec![
                message(0, Payload::Raw(trace_request(&["a"]).to_vec())),
                json_message(1, &span_json("b")),
            ],
        )
        .await
        .expect_err("the wrong payload must fail the batch");

        assert!(matches!(error, Error::SchemaMismatch(_)), "{error}");
        assert_eq!(mock.calls(), 1);
        assert_eq!(counter(&sink.counters.messages_sent), 1);
        assert_eq!(counter(&sink.counters.messages_dropped), 1);
    }

    #[tokio::test]
    async fn given_proto_mode_with_compression_when_consumed_should_forward_each_message() {
        let mock = MockReceiver::default();
        let (endpoint, _stop) = start_receiver(&mock);
        let mut config = test_config(&endpoint, OtlpSignal::Traces);
        config.format = StorageFormat::Proto;
        config.compression = true;
        let sink = open_sink(config).await;

        consume(
            &sink,
            vec![
                message(0, Payload::Raw(trace_request(&["a"]).to_vec())),
                message(1, Payload::Raw(trace_request(&["b", "c"]).to_vec())),
            ],
        )
        .await
        .unwrap();

        let received = mock.received.lock().unwrap();
        assert_eq!(received.traces.len(), 2);
        assert_eq!(counter(&sink.counters.messages_sent), 2);
    }

    #[tokio::test]
    async fn given_partial_success_with_rejections_when_consumed_should_fail_batch() {
        let mock = MockReceiver::with_replies(vec![Reply::Reject {
            rejected: 1,
            message: "invalid trace id",
        }]);
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;

        let error = consume(
            &sink,
            vec![
                json_message(0, &span_json("a")),
                json_message(1, &span_json("b")),
            ],
        )
        .await
        .expect_err("rejected records must fail the batch");

        assert!(matches!(error, Error::PermanentHttpError(_)), "{error}");
        assert_eq!(mock.calls(), 1, "a rejection is not retried");
        assert_eq!(counter(&sink.counters.records_rejected), 1);
        assert_eq!(counter(&sink.counters.messages_sent), 1);
    }

    #[tokio::test]
    async fn given_partial_success_warning_only_when_consumed_should_succeed() {
        let mock = MockReceiver::with_replies(vec![Reply::Reject {
            rejected: 0,
            message: "slow down",
        }]);
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;
        consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .unwrap();
        assert_eq!(counter(&sink.counters.records_rejected), 0);
    }

    #[tokio::test]
    async fn given_rejected_metrics_and_logs_when_consumed_should_fail_batch() {
        let mock = MockReceiver::with_replies(vec![
            Reply::Reject {
                rejected: 1,
                message: "m",
            },
            Reply::Reject {
                rejected: 1,
                message: "l",
            },
        ]);
        let (endpoint, _stop) = start_receiver(&mock);
        let metrics = open_sink(test_config(&endpoint, OtlpSignal::Metrics)).await;
        assert!(
            consume(
                &metrics,
                vec![json_message(0, r#"{"name":"cpu","value":1}"#)]
            )
            .await
            .is_err()
        );
        let logs = open_sink(test_config(&endpoint, OtlpSignal::Logs)).await;
        assert!(
            consume(&logs, vec![json_message(0, r#"{"body":"x"}"#)])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn given_transient_grpc_failure_when_consumed_should_retry_then_succeed() {
        let mock = MockReceiver::with_replies(vec![
            Reply::Fail(tonic::Code::Unavailable),
            Reply::Fail(tonic::Code::ResourceExhausted),
        ]);
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;
        consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .unwrap();
        assert_eq!(mock.calls(), 3);
        assert_eq!(counter(&sink.counters.messages_sent), 1);
    }

    #[tokio::test]
    async fn given_exhausted_grpc_retries_when_consumed_should_return_error() {
        let mock = MockReceiver::with_replies(vec![Reply::Fail(tonic::Code::Unavailable); 5]);
        let (endpoint, _stop) = start_receiver(&mock);
        let mut config = test_config(&endpoint, OtlpSignal::Traces);
        config.max_retries = Some(2);
        let sink = open_sink(config).await;
        let error = consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .expect_err("retries are exhausted");
        assert!(matches!(error, Error::Connection(_)), "{error}");
        assert_eq!(mock.calls(), 2);
        assert_eq!(counter(&sink.counters.messages_failed), 1);
    }

    #[tokio::test]
    async fn given_permanent_grpc_failure_when_consumed_should_not_retry() {
        let mock = MockReceiver::with_replies(vec![Reply::Fail(tonic::Code::InvalidArgument); 3]);
        let (endpoint, _stop) = start_receiver(&mock);
        let sink = open_sink(test_config(&endpoint, OtlpSignal::Traces)).await;
        let error = consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .expect_err("bad data is permanent");
        assert!(matches!(error, Error::PermanentHttpError(_)), "{error}");
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn given_proto_message_failing_midway_when_consumed_should_still_send_later_ones() {
        let mock = MockReceiver::with_replies(vec![
            Reply::Accept,
            Reply::Fail(tonic::Code::InvalidArgument),
            Reply::Accept,
        ]);
        let (endpoint, _stop) = start_receiver(&mock);
        let mut config = test_config(&endpoint, OtlpSignal::Traces);
        config.format = StorageFormat::Proto;
        let sink = open_sink(config).await;

        let messages = (0..3)
            .map(|offset| message(offset, Payload::Raw(trace_request(&["s"]).to_vec())))
            .collect();
        let error = consume(&sink, messages)
            .await
            .expect_err("the failed message must be reported");

        assert!(matches!(error, Error::PermanentHttpError(_)), "{error}");
        assert_eq!(mock.calls(), 3, "later messages are attempted");
        assert_eq!(counter(&sink.counters.messages_sent), 2);
        assert_eq!(counter(&sink.counters.messages_failed), 1);
    }

    fn http_config(server: &MockServer, signal: OtlpSignal) -> OtlpSinkConfig {
        let mut config = test_config(&server.uri(), signal);
        config.transport = Transport::Http;
        config
    }

    #[tokio::test]
    async fn given_http_transport_when_consumed_should_post_protobuf_with_headers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .and(header("content-type", "application/x-protobuf"))
            .and(header("x-scope-orgid", "tenant-1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = http_config(&server, OtlpSignal::Traces);
        config
            .headers
            .insert("x-scope-orgid".into(), SecretString::from("tenant-1"));
        let sink = open_sink(config).await;

        consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        let decoded = ExportTraceServiceRequest::decode(requests[0].body.as_slice()).unwrap();
        assert_eq!(decoded.resource_spans.len(), 1);
        assert_eq!(counter(&sink.counters.messages_sent), 1);
    }

    #[tokio::test]
    async fn given_http_compression_when_consumed_should_send_gzip_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("content-encoding", "gzip"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = http_config(&server, OtlpSignal::Logs);
        config.compression = true;
        let sink = open_sink(config).await;
        consume(&sink, vec![json_message(0, r#"{"body":"hello"}"#)])
            .await
            .unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(&requests[0].body[..2], &[0x1f, 0x8b]);
    }

    #[tokio::test]
    async fn given_http_4xx_when_consumed_should_return_permanent_error_without_retry() {
        for status in [400, 401, 404, 413] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status).set_body_string("nope"))
                .expect(1)
                .mount(&server)
                .await;
            let sink = open_sink(http_config(&server, OtlpSignal::Traces)).await;
            let error = consume(&sink, vec![json_message(0, &span_json("a"))])
                .await
                .expect_err("a 4xx must not be swallowed");
            assert!(
                matches!(error, Error::PermanentHttpError(_)),
                "{status}: {error}"
            );
            assert_eq!(counter(&sink.counters.messages_sent), 0);
            assert_eq!(counter(&sink.counters.batches_failed), 1);
        }
    }

    #[tokio::test]
    async fn given_http_429_then_success_when_consumed_should_retry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let sink = open_sink(http_config(&server, OtlpSignal::Traces)).await;
        consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn given_http_429_every_time_when_consumed_should_return_transient_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;
        let mut config = http_config(&server, OtlpSignal::Traces);
        config.max_retries = Some(2);
        let sink = open_sink(config).await;
        let error = consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .expect_err("throttling that never ends is a failure");
        assert!(matches!(error, Error::HttpRequestFailed(_)), "{error}");
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn given_http_redirect_when_consumed_should_not_follow_it() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(307).insert_header("location", "/elsewhere"))
            .expect(1)
            .mount(&server)
            .await;
        let sink = open_sink(http_config(&server, OtlpSignal::Traces)).await;
        let error = consume(&sink, vec![json_message(0, &span_json("a"))])
            .await
            .expect_err("a redirect is not a delivery");
        assert!(matches!(error, Error::PermanentHttpError(_)), "{error}");
    }

    #[tokio::test]
    async fn given_http_partial_success_when_consumed_should_fail_batch() {
        let server = MockServer::start().await;
        let body = ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: 2,
                error_message: "too old".into(),
            }),
        }
        .encode_to_vec();
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
        let sink = open_sink(http_config(&server, OtlpSignal::Traces)).await;
        let error = consume(
            &sink,
            vec![
                json_message(0, &span_json("a")),
                json_message(1, &span_json("b")),
            ],
        )
        .await
        .expect_err("rejections must fail the batch");
        assert!(matches!(error, Error::PermanentHttpError(_)), "{error}");
        assert_eq!(counter(&sink.counters.records_rejected), 2);
        assert_eq!(counter(&sink.counters.messages_sent), 0);
    }

    #[tokio::test]
    async fn given_http_proto_mode_with_wrong_payload_when_consumed_should_fail_batch() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = http_config(&server, OtlpSignal::Traces);
        config.format = StorageFormat::Proto;
        let sink = open_sink(config).await;
        let error = consume(
            &sink,
            vec![
                message(0, Payload::Raw(trace_request(&["a"]).to_vec())),
                message(1, Payload::Text("x".into())),
            ],
        )
        .await
        .expect_err("the wrong payload must fail the batch");
        assert!(matches!(error, Error::SchemaMismatch(_)), "{error}");
        assert_eq!(counter(&sink.counters.messages_sent), 1);
    }

    #[test]
    fn given_endpoint_with_credentials_should_redact_them_for_logs() {
        let redacted = redacted_endpoint("https://user:secret@qw:7281/path?token=abc");
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("token"));
        assert!(redacted.contains("qw:7281"));
    }

    #[test]
    fn given_new_sink_should_not_be_open_and_counters_should_be_zero() {
        let sink = OtlpSink::new(1, test_config("http://qw:7281", OtlpSignal::Logs));
        assert!(sink.connection.is_none());
        assert_eq!(counter(&sink.counters.messages_sent), 0);
        assert_eq!(counter(&sink.counters.batches_failed), 0);
    }
}
