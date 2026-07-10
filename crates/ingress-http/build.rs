// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    println!("cargo:rerun-if-changed=protobuf/ingestion_svc.proto");

    // Only generate (and thus require the `connectrpc` runtime + protoc) when the
    // `ingestion` feature is enabled. Build scripts cannot use `cfg(feature = ...)`,
    // so we gate on the env var Cargo sets for the feature instead.
    connectrpc_build::Config::new()
        .files(&["protobuf/ingestion_svc.proto"])
        .includes(&["protobuf/"])
        .include_file("_connectrpc.rs")
        .compile()
        .expect("failed to compile protobuf/ingestion_svc.proto");
}
