use std::fs;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;
use tonic::{Request, Response};
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::event_bus_service::Context;
use crate::event_reception_service::Event;
use crate::event_reception_service::event_reception_client::EventReceptionClient;
use crate::event_subscriber_service::event_subscriber_service_server::{EventSubscriberService, EventSubscriberServiceServer};
use crate::event_subscriber_service::Status;

trait Subscriber {
    fn notify(&self);
}

pub mod event_subscriber_service {
    tonic::include_proto!("event_subscriber_service");
}

pub mod event_reception_service {
    tonic::include_proto!("event_reception_service");
}

pub mod event_bus_service {
    tonic::include_proto!("event_bus_service");
}

pub async fn new_workflow(json: &Value, message: &mut Context, workflow_id: String) -> Event {
    let start = json.get("start")
        .expect("file should have FirstName key").as_str().unwrap();
    let step = json.get(start.to_string())
        .expect("file should have FirstName key").as_object().unwrap();
    message.step = Some(start.to_string());
    message.internal = None;
    message.output = None;
    message.workflow_id = Some(workflow_id);
    return Event {
        topic: "function".to_string(),
        resource_id: step.get("function").unwrap().as_str().unwrap().to_string(),
        context: Some(message.to_owned()),
    };
}

pub async fn existing_workflow(json: &Value, message: &mut Context) -> Event {
    let step = json.get(message.step.clone().unwrap())
        .expect("file should have FirstName key").as_object().unwrap();
    if step.get("next").is_some() {
        let next = step.get("next").unwrap().as_str().unwrap();
        let current = json.get(next)
            .expect("file should have FirstName key").as_object().unwrap();
        message.step = Some(next.to_string());
        return Event {
            topic: "function".to_string(),
            resource_id: current.get("function").unwrap().as_str().unwrap().to_string(),
            context: Some(message.to_owned()),
        };
    } else {
        message.internal = Some(true);
        message.output = Some(vec![]);
        message.step = None;
        return Event {
            topic: "result".to_string(),
            resource_id: "".to_string(),
            context: Some(message.to_owned()),
        };
    }
}


#[tonic::async_trait]
impl EventSubscriberService for FunctionEventSubscriber {
    async fn notify(&self, request: Request<event_subscriber_service::Message>) -> Result<Response<Status>, tonic::Status> {
        let req = request.into_inner();
        let mut message = req.context.unwrap();
        let clients_list = self.client.clone();
        tokio::spawn(async move {
            let file = fs::File::open(req.id.clone() + ".json")
                .expect("file should open read only");
            let json: Value = serde_json::from_reader(file)
                .expect("file should be proper JSON");
            if message.step.is_some() {
                let step = json.get(message.step.clone().unwrap())
                    .expect("file should have FirstName key").as_object().unwrap();
                if step.get("next").is_some() {
                    let next = step.get("next").unwrap().as_str().unwrap();
                    let current = json.get(next)
                        .expect("file should have FirstName key").as_object().unwrap();
                    message.step = Some(next.to_string());
                    let client1 = clients_list.read().await;
                    let mut client = client1.clone();
                    client.publish(Event {
                        topic: "function".to_string(),
                        resource_id: current.get("function").unwrap().as_str().unwrap().to_string(),
                        context: Some(message),
                    }).await.expect("Bus Not Available");
                } else {
                    message.internal = Some(true);
                    message.output = Some(vec![]);
                    message.step = None;
                    let client1 = clients_list.read().await;
                    let mut client = client1.clone();
                    client.publish(Event {
                        topic: "result".to_string(),
                        resource_id: "".to_string(),
                        context: Some(message),
                    }).await.expect("Bus Not Available");
                }
            } else {
                let start = json.get("start")
                    .expect("file should have FirstName key").as_str().unwrap();
                let step = json.get(start.to_string())
                    .expect("file should have FirstName key").as_object().unwrap();
                message.step = Some(start.to_string());
                message.internal = None;
                message.output = None;
                message.workflow_id = Some(req.id.clone());
                let client1 = clients_list.read().await;
                let mut client = client1.clone();
                client.publish(Event {
                    topic: "function".to_string(),
                    resource_id: step.get("function").unwrap().as_str().unwrap().to_string(),
                    context: Some(message),
                }).await.expect("Bus Not Available");
            }
        });
        return Ok(Response::new(Status {
            status: 1,
        }));
    }
}

struct FunctionEventSubscriber {
    client: Arc<tokio::sync::RwLock<EventReceptionClient<Channel>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).expect("no port given");
    let bus_port = std::env::args().nth(2).expect("no bus port given");

    let addr = format!("[::0]:{}", port.clone());
    let id = Uuid::new_v4().to_string();

    let greeter = FunctionEventSubscriber {
        client: Arc::new(RwLock::new(EventReceptionClient::connect(format!("http://localhost:{}", bus_port)).await.expect("Cannot connect to BUS").max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000)))
    };

    let mut client =
        EventReceptionClient::connect(format!("http://localhost:{}", bus_port)).await.expect("Cannot connect to BUS Reception").max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000);
    client.subscribe(event_reception_service::Subscriber {
        topic: "workflow".to_string(),
        id: id.clone(),
        url: format!("http://localhost:{}", port.clone()).to_string(),
    }).await.expect("Bus not available");

    Server::builder()
        .add_service(EventSubscriberServiceServer::new(greeter).max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000))
        .serve(addr.parse()?)
        .await?;

    Ok(())
}
