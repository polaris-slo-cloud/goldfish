use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::sync::watch::{Receiver, Sender};
use tonic::{Request, Response, Status};
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::event_bus_service::Context;
use crate::event_bus_service::event_bus_service_client::EventBusServiceClient;
use crate::event_reception_service::{Event, GetWarmActorRequest, InternalRequest, InternalResponse, RegisterRequest, SubscriberExits, TopicSubCount, UnregisterRequest};
use crate::event_reception_service::event_reception_client::EventReceptionClient;
use crate::event_reception_service::event_reception_server::{EventReception, EventReceptionServer};

pub mod event_reception_service {
    tonic::include_proto!("event_reception_service");
}

pub mod event_bus_service {
    tonic::include_proto!("event_bus_service");
}

static mut CLIENT_RECEPTION: Option<EventReceptionClient<Channel>> = None;

async fn handle_result(context: &mut Context, _port: String,
                       _execution_id: String,
                       function_requests: Arc<RwLock<HashMap<String, Mutex<Sender<Vec<u8>>>>>>,
                       client: Arc<RwLock<EventBusServiceClient<Channel>>>)
                       -> Result<Response<event_reception_service::Status>, Status> {
    return if (context.internal.is_none() || (context.internal.is_some() && !context.internal.unwrap()))
        && (context.step.is_none()) {
        Ok(Response::new(event_reception_service::Status {
            status: 1,
        }))
    } else if context.internal.is_some() && context.internal.unwrap() {
        let req1 = context.request_id.clone().unwrap();
        let context1 = context.clone();
        tokio::spawn(async move {
            let lock = function_requests.read().await;
            let mutex_val = lock.get(req1.as_str()).unwrap();
            let lock_result = mutex_val.lock().await;
            let output = context1.output.unwrap();
            lock_result.send(output).expect("TODO: panic message");
            lock_result.closed().await;
        });

        Ok(Response::new(event_reception_service::Status {
            status: 1,
        }))
    } else {
        let client1 = client.read().await;
        let mut client = client1.clone();
        let output = context.output.to_owned().unwrap();

        context.output = None;
        context.input = Some(output);
        client.publish(event_bus_service::Event {
            resource_id: context.workflow_id.clone().unwrap(),
            topic: "workflow".to_string(),
            context: Some(context.to_owned()),
        }).await.expect("Cannot Send Message to Bus");
        Ok(Response::new(event_reception_service::Status {
            status: 1,
        }))
    };
}

fn get_busses() -> Vec<Value> {
    println!("CALLED");
    let file = fs::File::open("../bus.json")
        .expect("file should open read only");
    let json: Value = serde_json::from_reader(file)
        .expect("file should be proper JSON");
    let busses = json.get("busses")
        .expect("file should have FirstName key").as_array().unwrap();
    return busses.clone();
}

async fn notify_bus(bus: Value, current_bus_id: String, topic: String, new_context: Context, resource_id: String) -> bool {
    let reception: String;
    let bus_val = bus.as_object().unwrap();
    if bus_val.get("bus").unwrap().as_str().unwrap().to_string() == current_bus_id {
        return false;
    }
    reception = bus_val.get("reception").unwrap().as_str().unwrap().to_string();
    let mut client;
    unsafe {
        if CLIENT_RECEPTION.is_none() {
            CLIENT_RECEPTION = Some(
                EventReceptionClient::connect(format!("http://localhost:{}", reception)).await.expect("Cannot connect to BUS"));
            client = CLIENT_RECEPTION.clone().unwrap();
        } else {
            client = CLIENT_RECEPTION.clone().unwrap();
        }
    }
    let response = client.check_contains_subscriber(event_reception_service::SubscriberExits {
        topic: topic.clone(),
    }).await.expect("Cannot Send Message to Bus");
    if response.into_inner().count != 0 {
        client.publish(event_reception_service::Event {
            resource_id,
            topic,
            context: Some(new_context),
        }).await.expect("Cannot Send Message to Bus");
        return true;
    }
    return false;
}


#[tonic::async_trait]
impl EventReception for ServerlessEventBus {
    async fn publish(&self, request: Request<event_reception_service::Event>) -> Result<Response<event_reception_service::Status>, Status> {
        let req = request.into_inner();
        let mut context = req.context.unwrap();

        if req.topic == "result" {
            let lock = self.function_requests.clone();

            return handle_result(&mut context, self.port.clone(), req.resource_id, lock, self.client.clone()).await;
        }

        let client1 = self.client.read().await;
        let mut client = client1.clone();
        let response = client.get_number_of_subs_for_topic(event_bus_service::SubscriberCount {
            topic: req.topic.to_string(),
        }).await.expect("Cannot Send Message to Bus");

        let mut found = false;
        context.bus_url = Some(format!("http://localhost:{}", self.reception_port.clone()));

        if response.into_inner().count > 0 {
            client.publish(event_bus_service::Event {
                resource_id: req.resource_id.to_string(),
                topic: req.topic.to_string(),
                context: Some(context),
            }).await.expect("Cannot Send Message to Bus");
            found = true
        } else {
            let busses = get_busses().into_iter();
            for bus in busses {
                found = notify_bus(bus, self.bus_id.clone(), req.topic.clone(),
                                   context.clone(), req.resource_id.clone()).await;
            }
        }
        if !found {
            return Ok(Response::new(event_reception_service::Status {
                status: 18,
            }));
        }

        return Ok(Response::new(event_reception_service::Status {
            status: 1,
        }));
    }

    async fn publish_http(&self, request: Request<Event>) -> Result<Response<event_reception_service::Status>, Status> {
        let req = request.into_inner();
        let mut context_json = req.context.unwrap();
        let id = Uuid::new_v4().to_string();
        context_json.request_id = Some(id.clone());
        context_json.internal = Some(true);
        let mut rec1: Receiver<Vec<u8>>;
        {
            let (tx, rx) = watch::channel(vec![]);
            let lock1 = self.function_requests.clone();
            let mut lock = lock1.write().await;

            lock.insert(id.clone(), Mutex::new(tx));
            rec1 = rx.clone();

            drop(lock);
        }
        self.publish(Request::new(Event {
            topic: req.topic.clone(),
            context: Some(context_json),
            resource_id: req.resource_id.clone(),
        })).await.expect("Cannot Publish");
        rec1.changed().await.expect("Channel not available");
        rec1.borrow_and_update();

        return Ok(Response::new(event_reception_service::Status {
            status: 1,
        }));
    }

    async fn subscribe(&self, request: Request<event_reception_service::Subscriber>) -> Result<Response<event_reception_service::Status>, Status> {
        let req = request.into_inner();
        let client1 = self.client.read().await;
        let mut client = client1.clone();

        client.subscribe(event_bus_service::Subscriber {
            topic: req.topic.to_string(),
            url: req.url.to_string(),
            id: req.id.to_string(),
        }).await.expect("Cannot Send Message to Bus");

        return Ok(Response::new(event_reception_service::Status {
            status: 1,
        }));
    }

    async fn register_warm_actor(&self, request: Request<RegisterRequest>) -> Result<Response<event_reception_service::Status>, Status> {
        let req = request.into_inner();
        let client1 = self.client.read().await;
        let mut client = client1.clone();

        client.register_warm_actor(event_bus_service::RegisterRequest {
            url: req.url.clone(),
            function_id: req.function_id.clone(),
        }).await.expect("Connection not available");
        return Ok(Response::new(event_reception_service::Status {
            status: 1,
        }));
    }

    async fn unregister_warm_actor(&self, request: Request<UnregisterRequest>) -> Result<Response<event_reception_service::Status>, Status> {
        let req = request.into_inner();
        let client1 = self.client.read().await;
        let mut client = client1.clone();

        client.unregister_warm_actor(event_bus_service::UnregisterRequest {
            url: req.url.clone(),
            function_id: req.function_id.clone(),
        }).await.expect("Connection not available");
        return Ok(Response::new(event_reception_service::Status {
            status: 1,
        }));
    }

    async fn get_warm_actor(&self, request: Request<GetWarmActorRequest>) -> Result<Response<event_reception_service::Status>, Status> {
        let req = request.into_inner();

        let client1 = self.client.read().await;
        let mut client = client1.clone();

        client.get_warm_actor(event_bus_service::GetWarmActorRequest {
            function_id: req.function_id.clone()
        }).await.expect("Connection not available");

        return Ok(Response::new(event_reception_service::Status {
            status: 1,
        }));
    }

    async fn check_contains_subscriber(&self, request: Request<SubscriberExits>) -> Result<Response<TopicSubCount>, Status> {
        let req = request.into_inner();

        let client1 = self.client.read().await;
        let mut client = client1.clone();

        let response = client.get_number_of_subs_for_topic(event_bus_service::SubscriberCount {
            topic: req.topic.to_string(),
        }).await.expect("Cannot Send Message to Bus");

        return Ok(Response::new(event_reception_service::TopicSubCount {
            count: response.into_inner().count,
        }));
    }

    async fn call_function_internally(&self, request: Request<InternalRequest>) -> Result<Response<InternalResponse>, Status> {
        let req = request.into_inner();

        let client1 = self.client.read().await;
        let mut client = client1.clone();
        let response = client.get_number_of_subs_for_topic(event_bus_service::SubscriberCount {
            topic: "function".to_string(),
        }).await.expect("Cannot Send Message to Bus");

        let id = Uuid::new_v4().to_string();

        let mut rec1: Receiver<Vec<u8>>;
        {
            let (tx, rx) = watch::channel(vec![]);
            let lock1 = self.function_requests.clone();
            let mut lock = lock1.write().await;
            lock.insert(id.clone(), Mutex::new(tx));
            rec1 = rx.clone();
            drop(lock);
        }
        if response.into_inner().count > 0 {
            let context10001 = Context {
                bus_url: Some(format!("http://localhost:{}", self.reception_port.clone())),
                input: Some(req.context),
                request_id: Some(id.clone()),
                internal: Some(true),
                step: None,
                workflow_id: None,
                output: None,
                id: None,
            };
            client.publish(event_bus_service::Event {
                resource_id: req.resource_id.to_string(),
                topic: "function".to_string(),
                context: Some(context10001),
            }).await.expect("Cannot Send Message to Bus");
        } else {
            let context1000 = Context {
                bus_url: Some(format!("http://localhost:{}", self.reception_port.clone())),
                input: Some(req.context),
                request_id: Some(id.clone()),
                internal: Some(true),
                step: None,
                workflow_id: None,
                output: None,
                id: None,
            };
            let busses = get_busses().into_iter();
            for bus in busses {
                notify_bus(bus, self.bus_id.clone(), "function".to_string(),
                           context1000.clone(), req.resource_id.clone()).await;
            }
        }

        rec1.changed().await.expect("TODO: panic message");
        let vals10 = rec1.borrow_and_update();

        return Ok(Response::new(event_reception_service::InternalResponse {
            source_id: req.source_id.clone().to_string(),
            resource_id: req.resource_id.clone().to_string(),
            context: vals10.to_owned(),
            id: id.clone().to_string(),
        }));
    }
}


struct ServerlessEventBus {
    bus_id: String,
    port: String,
    reception_port: String,
    function_requests: Arc<RwLock<HashMap<String, Mutex<Sender<Vec<u8>>>>>>,
    client: Arc<tokio::sync::RwLock<EventBusServiceClient<Channel>>>,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\x1b[92m Event Reception Running \x1b[0m");
    let id = std::env::args().nth(1).expect("no bus given");
    let file = fs::File::open("../bus.json")
        .expect("file should open read only");
    let json: Value = serde_json::from_reader(file)
        .expect("file should be proper JSON");
    let busses = json.get("busses")
        .expect("file should have FirstName key").as_array().unwrap().into_iter();
    let mut bus_id = "".to_string();
    let mut port = "".to_string();
    let mut reception = "".to_string();
    for b in busses {
        if b.as_object().unwrap().get("bus").unwrap().as_str().unwrap().to_string() == id {
            port = b.as_object().unwrap().get("port").unwrap().as_str().unwrap().to_string();
            reception = b.as_object().unwrap().get("reception").unwrap().as_str().unwrap().to_string();
            bus_id = b.as_object().unwrap().get("bus").unwrap().as_str().unwrap().to_string();
            break;
        }
    }
    let greeter = ServerlessEventBus {
        reception_port: reception.clone(),
        port: port.clone(),
        bus_id,
        function_requests: Arc::new(RwLock::new(HashMap::new())),
        client: Arc::new(RwLock::new(
            EventBusServiceClient::connect(format!("http://localhost:{}", port.clone())).await.expect("Cannot connect to BUS"))
        ),
    };

    let addr = format!("[::0]:{}", reception).parse()?;

    Server::builder().concurrency_limit_per_connection(500)
        .add_service(EventReceptionServer::new(greeter).max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000))
        .serve(addr)
        .await?;

    Ok(())
}
