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

use super::receiver::{OtlpGrpcReceiver, OtlpHttpReceiver};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture, seeds};
use std::collections::HashMap;

const ENV_PLUGIN_ENDPOINT: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_ENDPOINT";
const ENV_PLUGIN_SIGNAL: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_SIGNAL";
const ENV_PLUGIN_TRANSPORT: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_TRANSPORT";
const ENV_PLUGIN_FORMAT: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_FORMAT";
const ENV_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_OTLP_STREAMS_0_STREAM";
const ENV_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_OTLP_STREAMS_0_TOPICS";
const ENV_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_OTLP_STREAMS_0_SCHEMA";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_OTLP_PATH";

fn base_envs(
    endpoint: String,
    transport: &str,
    format: &str,
    schema: &str,
) -> HashMap<String, String> {
    HashMap::from([
        (ENV_PLUGIN_ENDPOINT.to_string(), endpoint),
        (ENV_PLUGIN_SIGNAL.to_string(), "traces".to_string()),
        (ENV_PLUGIN_TRANSPORT.to_string(), transport.to_string()),
        (ENV_PLUGIN_FORMAT.to_string(), format.to_string()),
        (
            ENV_STREAMS_0_STREAM.to_string(),
            seeds::names::STREAM.to_string(),
        ),
        (
            ENV_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", seeds::names::TOPIC),
        ),
        (ENV_STREAMS_0_SCHEMA.to_string(), schema.to_string()),
        (
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_otlp_sink".to_string(),
        ),
    ])
}

/// JSON documents over OTLP/gRPC.
pub struct OtlpSinkGrpcFixture {
    receiver: OtlpGrpcReceiver,
}

impl OtlpSinkGrpcFixture {
    pub fn receiver(&self) -> &OtlpGrpcReceiver {
        &self.receiver
    }
}

#[async_trait]
impl TestFixture for OtlpSinkGrpcFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            receiver: OtlpGrpcReceiver::start()?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(self.receiver.endpoint(), "grpc", "json", "json")
    }
}

/// Raw `ExportTraceServiceRequest` messages over OTLP/gRPC.
pub struct OtlpSinkGrpcProtoFixture {
    receiver: OtlpGrpcReceiver,
}

impl OtlpSinkGrpcProtoFixture {
    pub fn receiver(&self) -> &OtlpGrpcReceiver {
        &self.receiver
    }
}

#[async_trait]
impl TestFixture for OtlpSinkGrpcProtoFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            receiver: OtlpGrpcReceiver::start()?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(self.receiver.endpoint(), "grpc", "proto", "raw")
    }
}

/// JSON documents over OTLP/HTTP.
pub struct OtlpSinkHttpFixture {
    receiver: OtlpHttpReceiver,
}

impl OtlpSinkHttpFixture {
    pub fn receiver(&self) -> &OtlpHttpReceiver {
        &self.receiver
    }
}

#[async_trait]
impl TestFixture for OtlpSinkHttpFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            receiver: OtlpHttpReceiver::start().await,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        base_envs(self.receiver.endpoint(), "http", "json", "json")
    }
}
