/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # MIMDB Server
//!
//! HTTP REST API server for the MIMDB database system.
//!
//! ## Usage
//!
//! ```bash
//! # Start server with default settings
//! cargo run --bin server
//!
//! # Start server with custom data directory
//! cargo run --bin server -- --data-dir /path/to/data
//!
//! # Start server on custom port
//! cargo run --bin server -- --port 8080
//! ```

use axum::Router;
use mimdb::api::executor::QueryExecutor;
use mimdb::api::handlers::AppState;
use mimdb::api::handlers::create_routes;
use mimdb::api::swagger::create_swagger_routes;
use mimdb::metastore::Metastore;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::Level;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

const DEFAULT_PORT: u16 = 3000;
const DEFAULT_DATA_DIR: &str = "./mimdb_data";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let mut port = DEFAULT_PORT;
    let mut data_dir = PathBuf::from(DEFAULT_DATA_DIR);

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse().expect("Invalid port number");
                    i += 2;
                } else {
                    eprintln!("Error: --port requires a value");
                    std::process::exit(1);
                }
            }
            "--data-dir" | "-d" => {
                if i + 1 < args.len() {
                    data_dir = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --data-dir requires a value");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                println!(
                    "MIMDB Server - Columnar Analytical Database\n\n\
                     USAGE:\n\
                     \tserver [OPTIONS]\n\n\
                     OPTIONS:\n\
                     \t-p, --port <PORT>         \tPort to listen on (default: {})\n\
                     \t-d, --data-dir <PATH>     \tData directory path (default: {})\n\
                     \t-h, --help                \tShow this help message",
                    DEFAULT_PORT, DEFAULT_DATA_DIR
                );
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
    }

    // Initialize metastore
    let metastore = Arc::new(Metastore::new(&data_dir).expect("Failed to initialize metastore"));

    // Initialize query executor
    let executor = Arc::new(QueryExecutor::new(Arc::clone(&metastore)));

    // Create application state
    let app_state = Arc::new(AppState {
        metastore,
        executor,
        start_time: chrono::Utc::now(),
    });

    // Build the router
    let app = Router::new()
        .merge(create_routes())
        .merge(create_swagger_routes())
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    info!("Starting MIMDB server on port {}", port);
    info!("Data directory: {:?}", data_dir);
    info!(
        "Swagger UI available at http://localhost:{}/swagger-ui",
        port
    );

    // Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
