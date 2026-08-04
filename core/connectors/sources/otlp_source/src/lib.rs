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
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::{BatchPolicy, SourceBatchResult},
    source_connector,
};
use serde::Deserialize;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tracing::{info, warn};

pub(crate) mod convert;
pub(crate) mod server;

source_connector!(OtlpSource);

/// How messages are stored in the Iggy topic.
///
/// `Json` (default): each OTLP signal item (span / data-point / log record)
/// is stored as a flat JSON document. Human-readable but verbose.
///
/// `Proto`: the entire `Export*ServiceRequest` proto is stored as raw bytes
/// per gRPC call. Typically 4-5x smaller than JSON and zero-copy on the sink
/// side when paired with `otlp_sink`'s `format = "proto"`.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageFormat {
    #[default]
    Json,
    Proto,
}

#[derive(Debug)]
pub struct OtlpSource {
    id: u32,
    config: OtlpSourceConfig,
    rx: Mutex<Option<mpsc::Receiver<ProducedMessage>>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    server_task: Mutex<Option<JoinHandle<()>>>,
    /// Batch handed to the runtime and not yet acknowledged. Exports were
    /// already answered as accepted, so a NACKed batch exists nowhere else and
    /// the next `poll()` has to replay it.
    staged: Mutex<Option<Vec<ProducedMessage>>>,
    /// Consecutive NACKs the staged batch has taken.
    nacks: AtomicU32,
}

/// Configuration for the OTLP source connector, deserialized from
/// `[plugin_config]` in config.toml.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpSourceConfig {
    /// Address the gRPC server binds to, e.g. `0.0.0.0:4317`.
    pub listen_addr: String,
    /// In-process buffer between the gRPC server and `poll()` (default: 50000).
    /// Must be greater than 0. Overflowing exports are reported back to the
    /// client through `partial_success` rather than failing the whole batch.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    /// Maximum messages returned by a single `poll()` (default: 1000).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// How records are stored, see [`StorageFormat`] (default: `json`).
    #[serde(default)]
    pub format: StorageFormat,
    /// Consecutive NACKed batches after which the SDK stops the source.
    /// Omitted (default): keep replaying. Accepted exports live only in memory,
    /// so stopping on a short Iggy outage would end ingress until a restart.
    #[serde(default)]
    pub max_consecutive_nacks: Option<u32>,
}

fn default_channel_capacity() -> usize {
    50_000
}

fn default_batch_size() -> usize {
    1_000
}

impl OtlpSource {
    pub fn new(id: u32, config: OtlpSourceConfig, _state: Option<ConnectorState>) -> Self {
        OtlpSource {
            id,
            config,
            rx: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            server_task: Mutex::new(None),
            staged: Mutex::new(None),
            nacks: AtomicU32::new(0),
        }
    }
}

fn replica(message: &ProducedMessage) -> ProducedMessage {
    ProducedMessage {
        id: message.id,
        checksum: message.checksum,
        timestamp: message.timestamp,
        origin_timestamp: message.origin_timestamp,
        headers: message.headers.clone(),
        payload: message.payload.clone(),
    }
}

#[async_trait]
impl Source for OtlpSource {
    async fn open(&mut self) -> Result<(), Error> {
        if self.rx.lock().await.is_some() {
            return Err(Error::InitError(
                "OTLP source connector is already open".to_string(),
            ));
        }

        // tokio::sync::mpsc::channel panics on a zero capacity, which would take
        // down the whole runtime process on a typo in the connector TOML.
        if self.config.channel_capacity == 0 {
            return Err(Error::InvalidConfigValue(
                "channel_capacity must be greater than 0".to_string(),
            ));
        }

        if self.config.max_consecutive_nacks == Some(0) {
            return Err(Error::InvalidConfigValue(
                "max_consecutive_nacks must be greater than 0".to_string(),
            ));
        }

        let addr = self
            .config
            .listen_addr
            .parse()
            .map_err(|err| Error::InitError(format!("Invalid listen address: {err}")))?;

        let incoming = TcpIncoming::bind(addr).map_err(|err| {
            Error::InitError(format!("Failed to bind {}: {err}", self.config.listen_addr))
        })?;

        info!(
            "OTLP source connector with ID: {} listening on {}",
            self.id, self.config.listen_addr
        );

        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(server::run_grpc_server(
            incoming,
            tx,
            shutdown_rx,
            self.config.format,
        ));

        *self.rx.lock().await = Some(rx);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);
        *self.server_task.lock().await = Some(handle);

        Ok(())
    }

    fn batch_policy(&self) -> BatchPolicy {
        BatchPolicy::default()
            .with_max_consecutive_nacks(self.config.max_consecutive_nacks.and_then(NonZeroU32::new))
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let schema = match self.config.format {
            StorageFormat::Json => Schema::Json,
            StorageFormat::Proto => Schema::Raw,
        };

        if let Some(staged) = self.staged.lock().await.as_ref() {
            return Ok(ProducedMessages {
                schema,
                messages: staged.iter().map(replica).collect(),
                state: None,
            });
        }

        let mut rx_guard = self.rx.lock().await;
        let rx = rx_guard.as_mut().ok_or_else(|| {
            Error::InitError("OTLP source connector is not initialized".to_string())
        })?;

        let first = match rx.recv().await {
            Some(msg) => msg,
            None => {
                return Err(Error::Connection(
                    "OTLP gRPC server terminated unexpectedly".to_string(),
                ));
            }
        };

        let mut messages = Vec::with_capacity(self.config.batch_size);
        messages.push(first);

        while messages.len() < self.config.batch_size {
            match rx.try_recv() {
                Ok(msg) => messages.push(msg),
                Err(_) => break,
            }
        }

        *self.staged.lock().await = Some(messages.iter().map(replica).collect());

        Ok(ProducedMessages {
            schema,
            messages,
            state: None,
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let mut staged = self.staged.lock().await;
        match result {
            SourceBatchResult::Ack => {
                *staged = None;
                self.nacks.store(0, Ordering::Release);
            }
            SourceBatchResult::Nack => {
                let nacks = self.nacks.fetch_add(1, Ordering::AcqRel) + 1;
                warn!(
                    "Runtime NACKed {} messages ({nacks} so far) for OTLP source connector with ID: {}, replaying",
                    staged.as_ref().map_or(0, Vec::len),
                    self.id
                );
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        let task = self.server_task.lock().await.take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        *self.rx.lock().await = None;
        if let Some(unsent) = self.staged.lock().await.take() {
            warn!(
                "OTLP source connector with ID: {} closed with {} unacknowledged messages",
                self.id,
                unsent.len()
            );
        }
        info!("OTLP source connector with ID: {} closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_minimal_config_should_apply_defaults() {
        let config: OtlpSourceConfig =
            serde_json::from_str(r#"{"listen_addr": "0.0.0.0:4317"}"#).expect("config parses");

        assert_eq!(config.channel_capacity, 50_000);
        assert_eq!(config.batch_size, 1_000);
        assert!(matches!(config.format, StorageFormat::Json));
        assert_eq!(config.max_consecutive_nacks, None);
    }

    #[test]
    fn given_lowercase_format_should_deserialize() {
        let config: OtlpSourceConfig =
            serde_json::from_str(r#"{"listen_addr": "0.0.0.0:4317", "format": "proto"}"#)
                .expect("config parses");

        assert!(matches!(config.format, StorageFormat::Proto));
    }

    #[test]
    fn given_unknown_config_field_should_fail_deserialization() {
        // Without deny_unknown_fields a typo would silently fall back to the
        // default and the connector would run on a configuration nobody wrote.
        let result: Result<OtlpSourceConfig, _> =
            serde_json::from_str(r#"{"listen_addr": "0.0.0.0:4317", "batch_sise": 10}"#);

        let error = result.expect_err("unknown field is rejected");
        assert!(
            error.to_string().contains("batch_sise"),
            "error names the offending key, got: {error}"
        );
    }

    #[test]
    fn given_missing_listen_addr_should_fail_deserialization() {
        let result: Result<OtlpSourceConfig, _> = serde_json::from_str("{}");

        assert!(result.is_err(), "listen_addr has no default");
    }

    fn test_source() -> OtlpSource {
        let config: OtlpSourceConfig =
            serde_json::from_str(r#"{"listen_addr": "127.0.0.1:0", "batch_size": 2}"#)
                .expect("config parses");
        OtlpSource::new(1, config, None)
    }

    fn message(payload: &[u8]) -> ProducedMessage {
        ProducedMessage {
            id: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: payload.to_vec(),
        }
    }

    async fn source_with_queue(payloads: &[&[u8]]) -> OtlpSource {
        let source = test_source();
        let (tx, rx) = mpsc::channel(8);
        for payload in payloads {
            tx.try_send(message(payload)).expect("channel has room");
        }
        // The sender stays alive in a leaked clone only for the test body.
        std::mem::forget(tx);
        *source.rx.lock().await = Some(rx);
        source
    }

    fn payloads(batch: &ProducedMessages) -> Vec<Vec<u8>> {
        batch.messages.iter().map(|m| m.payload.clone()).collect()
    }

    #[tokio::test]
    async fn given_nack_should_replay_identical_batch() {
        let source = source_with_queue(&[b"a", b"b", b"c"]).await;

        let first = source.poll().await.expect("poll");
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack handled");
        let second = source.poll().await.expect("replay");

        assert_eq!(payloads(&first), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(payloads(&second), payloads(&first));
    }

    #[tokio::test]
    async fn given_ack_should_release_batch_and_poll_next() {
        let source = source_with_queue(&[b"a", b"b", b"c"]).await;

        source.poll().await.expect("poll");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack handled");
        let next = source.poll().await.expect("next batch");

        assert_eq!(payloads(&next), vec![b"c".to_vec()]);
        assert!(source.staged.lock().await.is_some());
    }

    #[test]
    fn given_default_config_should_not_limit_nacks() {
        let source = test_source();

        assert_eq!(source.batch_policy().max_consecutive_nacks(), None);
    }

    #[test]
    fn given_configured_limit_should_reach_batch_policy() {
        let config: OtlpSourceConfig =
            serde_json::from_str(r#"{"listen_addr": "127.0.0.1:0", "max_consecutive_nacks": 7}"#)
                .expect("config parses");
        let source = OtlpSource::new(1, config, None);

        assert_eq!(
            source.batch_policy().max_consecutive_nacks(),
            NonZeroU32::new(7)
        );
    }
}
