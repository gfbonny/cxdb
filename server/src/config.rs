// Copyright 2025 StrongDM Inc
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind_addr: String,
    pub http_bind_addr: String,
}

impl Config {
    pub fn from_env() -> Self {
        let data_dir = env::var("CXDB_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
        let bind_addr = env::var("CXDB_BIND").unwrap_or_else(|_| "127.0.0.1:9009".to_string());
        let http_bind_addr =
            env::var("CXDB_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:9010".to_string());
        Self {
            data_dir: PathBuf::from(data_dir),
            bind_addr,
            http_bind_addr,
        }
    }
}
