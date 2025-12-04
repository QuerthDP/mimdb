/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # REST API Module
//!
//! This module provides the REST API for the MIMDB database system,
//! implementing the interface defined in dbmsInterface.yaml.

pub mod executor;
pub mod handlers;
pub mod models;
pub mod swagger;

/// OpenAPI specification embedded in the binary
pub(crate) const OPENAPI_SPEC: &str = include_str!("../../../api/dbmsInterface.yaml");
