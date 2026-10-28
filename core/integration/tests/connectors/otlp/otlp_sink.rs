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

use crate::connectors::fixtures::{
    GRPC_INDEX_HEADER, OtlpSinkGrpcFixture, OtlpSinkGrpcProtoFixture, OtlpSinkHttpFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use serde_json::json;

const SPAN_COUNT: usize = 5;
const TRACE_ID: &str = "0102030405060708090a0b0c0d0e0f10";

fn span_documents() -> Vec<serde_json::Value> {
    (0..SPAN_COUNT)
        .map(|i| {
            json!({
                "signal": "trace",
                "trace_id": TRACE_ID,
                "span_id": format!("{:016x}", i + 1),
                "name": format!("span-{i}"),
                "kind": "server",
                "start_time_ns": 1_700_000_000_000_000_000u64 + i as u64,
                "end_time_ns": 1_700_000_000_000_001_000u64 + i as u64,
                "status": "ok",
                "service_name": "api",
                "resource": { "service.name": "api" },
                "attributes": { "http.method": "GET" }
            })
        })
        .collect()
}

async fn send(harness: &TestHarness, payloads: Vec<Bytes>) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let mut messages: Vec<IggyMessage> = payloads
        .into_iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(payload)
                .build()
                .expect("build message")
        })
        .collect();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");
}

fn assert_spans(spans: &[Span]) {
    let mut names: Vec<&str> = spans.iter().map(|span| span.name.as_str()).collect();
    names.sort_unstable();
    let expected: Vec<String> = (0..SPAN_COUNT).map(|i| format!("span-{i}")).collect();
    assert_eq!(names, expected);
    for span in spans {
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.span_id.len(), 8);
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_spans_should_reach_the_grpc_receiver_with_configured_headers(
    harness: &TestHarness,
    fixture: OtlpSinkGrpcFixture,
) {
    let payloads = span_documents()
        .iter()
        .map(|document| Bytes::from(serde_json::to_vec(document).expect("serialize")))
        .collect();
    send(harness, payloads).await;

    let spans = fixture
        .receiver()
        .wait_for_spans(SPAN_COUNT)
        .await
        .expect("spans");

    assert_spans(&spans);
    let index_headers = fixture.receiver().index_headers();
    assert!(
        !index_headers.is_empty(),
        "{GRPC_INDEX_HEADER} must be sent"
    );
    assert!(
        index_headers
            .iter()
            .all(|value| value == "otel-traces-v0_7")
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_proto_messages_should_be_forwarded_unchanged_over_grpc(
    harness: &TestHarness,
    fixture: OtlpSinkGrpcProtoFixture,
) {
    let spans: Vec<Span> = (0..SPAN_COUNT)
        .map(|i| Span {
            trace_id: vec![7; 16],
            span_id: vec![i as u8 + 1; 8],
            name: format!("span-{i}"),
            ..Default::default()
        })
        .collect();
    let payloads: Vec<Bytes> = spans
        .iter()
        .map(|span| {
            Bytes::from(
                ExportTraceServiceRequest {
                    resource_spans: vec![ResourceSpans {
                        scope_spans: vec![ScopeSpans {
                            spans: vec![span.clone()],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                }
                .encode_to_vec(),
            )
        })
        .collect();
    send(harness, payloads).await;

    let received = fixture
        .receiver()
        .wait_for_spans(SPAN_COUNT)
        .await
        .expect("spans");

    assert_spans(&received);
    assert!(received.iter().all(|span| span.trace_id == vec![7; 16]));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/otlp/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_spans_should_reach_the_http_receiver(
    harness: &TestHarness,
    fixture: OtlpSinkHttpFixture,
) {
    let payloads = span_documents()
        .iter()
        .map(|document| Bytes::from(serde_json::to_vec(document).expect("serialize")))
        .collect();
    send(harness, payloads).await;

    let spans = fixture
        .receiver()
        .wait_for_spans(SPAN_COUNT)
        .await
        .expect("spans");

    assert_spans(&spans);
}
