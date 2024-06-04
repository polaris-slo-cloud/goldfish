use std::sync::Arc;

use axum::{http::StatusCode, Json, Router, routing::post};
use axum::extract::{DefaultBodyLimit, State};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::event_bus_service::Context;
use crate::event_reception_service::Event;
use crate::event_reception_service::event_reception_client::EventReceptionClient;

pub mod event_reception_service {
    tonic::include_proto!("event_reception_service");
}

pub mod event_bus_service {
    tonic::include_proto!("event_bus_service");
}

#[derive(Clone)]
struct CustomBus {
    client: Arc<tokio::sync::RwLock<EventReceptionClient<Channel>>>,
}

#[tokio::main]
async fn main() {
    let client_channel = CustomBus {
        client: Arc::new(tokio::sync::RwLock::new(EventReceptionClient::connect("http://localhost:50058")
            .await.expect("Cannot connect to BUS").max_encoding_message_size(100 * 1000000).max_decoding_message_size(100 * 1000000))
        )
    };
    let app = Router::new()
        .route("/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(100 * 1000000))
        .with_state(client_channel).layer(DefaultBodyLimit::max(100 * 1000000));
    let listener = tokio::net::TcpListener::bind("localhost:8080")
        .await
        .unwrap();
    axum::serve(listener, app.into_make_service()).await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct A2 {
    input: String,
}

#[derive(Deserialize)]
struct EventRequest {
    topic: String,
    context: A2,
    resource_id: String,
}

async fn invoke(State(app): State<CustomBus>, request: Json<EventRequest>) -> Result<String, (StatusCode, String)> {
    let client1 = app.client.read().await;
    let response = client1.clone().publish_http(Event {
        topic: request.topic.clone(),
        context: Some(Context {
            bus_url: None,
            input: Some(request.context.input.as_bytes().to_vec()),
            request_id: None,
            internal: None,
            step: None,
            workflow_id: None,
            output: None,
            id: None,
        }),
        resource_id: request.resource_id.clone(),
    }).await.expect("Connection not available");
    response.into_inner().status;
    Ok("READY".to_string())
}