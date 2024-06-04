use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use crate::event_bus::{EventBus, EventStruct, EventSubscriber, ServerlessEventBus};
use crate::event_bus_service::{Event, GetWarmActorRequest, RegisterRequest, Subscriber, SubscriberCount, TopicSubCount, UnregisterRequest};
use crate::event_bus_service::event_bus_service_server::{EventBusService, EventBusServiceServer};

mod event_bus;

pub mod event_bus_service {
    tonic::include_proto!("event_bus_service");
}

pub mod event_subscriber_service {
    tonic::include_proto!("event_subscriber_service");
}

#[tonic::async_trait]
impl EventBusService for ServerlessEventBus {
    async fn publish(
        &self,
        request: Request<Event>,
    ) -> Result<Response<event_bus_service::Status>, Status> {
        let req = request.into_inner().clone();
        let event = EventStruct {
            resource_id: req.resource_id.clone(),
            context: req.context,
            topic: req.topic.clone(),
        };
        self.publish_event(&event).await;
        return Ok(Response::new(event_bus_service::Status { status: 1 }));
    }

    async fn subscribe(
        &self,
        request: Request<Subscriber>,
    ) -> Result<Response<event_bus_service::Status>, Status> {
        let req = request.into_inner().clone();
        self.subscribe_event(&EventSubscriber {
            url: req.url.to_string(),
            topic: req.topic.clone(),
            client: None,
        })
            .await;

        return Ok(Response::new(event_bus_service::Status { status: 1 }));
    }

    async fn register_warm_actor(&self, request: Request<RegisterRequest>) -> Result<Response<event_bus_service::Status>, Status> {
        let req = request.into_inner();
        self.register_function(req.function_id.clone(), req.url.as_str()).await;
        return Ok(Response::new(event_bus_service::Status { status: 1 }));
    }

    async fn unregister_warm_actor(&self, request: Request<UnregisterRequest>) -> Result<Response<event_bus_service::Status>, Status> {
        let req = request.into_inner();
        self.unregister_function(req.function_id.clone(), req.url.as_str()).await;
        return Ok(Response::new(event_bus_service::Status { status: 1 }));
    }

    async fn get_warm_actor(&self, request: Request<GetWarmActorRequest>) -> Result<Response<event_bus_service::Status>, Status> {
        let req = request.into_inner();
        self.get_warm_function(req.function_id.as_str()).await;
        return Ok(Response::new(event_bus_service::Status { status: 1 }));
    }

    async fn get_number_of_subs_for_topic(
        &self,
        request: Request<SubscriberCount>,
    ) -> Result<Response<TopicSubCount>, Status> {
        let req = request.into_inner().clone();
        let count = self
            .get_topic_sub_count(req.topic.as_str())
            .await;

        return Ok(Response::new(TopicSubCount {
            count: count as u64,
        }));
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\x1b[92m[Event Bus Started]\x1b[0m");
    let id = std::env::args().nth(1).expect("no bus given");
    println!("\x1b[92m[BUS {}]\x1b[0m ", id.clone());
    let file = fs::File::open("../bus.json").expect("file should open read only");
    let json: serde_json::Value =
        serde_json::from_reader(file).expect("file should be proper JSON");
    let busses = json
        .get("busses")
        .expect("file should have FirstName key")
        .as_array()
        .unwrap()
        .into_iter();
    let mut port = "".to_string();
    for b in busses {
        if b.as_object()
            .unwrap()
            .get("bus")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
            == id
        {
            port = b
                .as_object()
                .unwrap()
                .get("port")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string();
            break;
        }
    }
    let (t1, mut r1) = unbounded_channel();
    let subs = Arc::new(RwLock::new(HashMap::new()));
    let warm_funcs = Arc::new(RwLock::new(HashMap::new()));
    let greeter = ServerlessEventBus {
        events_sender: t1,
        events: Arc::new(RwLock::new(HashMap::new())),
        subscribers: subs.clone(),
        warm_functions: warm_funcs.clone(),
    };
    tokio::spawn(async move {
        while let Some(msg) = r1.recv().await {
            let subs1 = subs.clone();
            let warm_funcs = warm_funcs.clone();
            tokio::spawn(async move {
                event_bus::run_subscriber(subs1.clone(), msg, warm_funcs.clone()).await;
            });
        }
    });
    let addr = format!("[::0]:{}", port).parse()?;

    Server::builder()
        .add_service(EventBusServiceServer::new(greeter).max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000))
        .serve(addr)
        .await?;

    Ok(())
}
