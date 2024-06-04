use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use rand::seq::SliceRandom;
use shiplift::{ContainerOptions, Docker};
use tokio::sync::RwLock;
use tonic::{Request, Response};
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::actor_service::actor_service_client::ActorServiceClient;
use crate::actor_service::RunRequest;
use crate::event_reception_service::event_reception_client::EventReceptionClient;
use crate::event_subscriber_service::event_subscriber_service_server::{EventSubscriberService, EventSubscriberServiceServer};
use crate::event_subscriber_service::Status;

trait Subscriber {
    fn notify(&self);
}

pub mod event_subscriber_service {
    tonic::include_proto!("event_subscriber_service");
}

pub mod event_bus_service {
    tonic::include_proto!("event_bus_service");
}

pub mod event_reception_service {
    tonic::include_proto!("event_reception_service");
}

pub mod actor_service {
    tonic::include_proto!("actor_service");
}

fn port_is_available(port: u16) -> bool {
    match TcpListener::bind(("127.0.0.1", port)) {
        Ok(_) => true,
        Err(_) => false,
    }
}

#[tonic::async_trait]
impl EventSubscriberService for FunctionEventSubscriber {
    async fn notify(&self, request: Request<event_subscriber_service::Message>) -> Result<Response<Status>, tonic::Status> {
        let req = request.into_inner().clone();

        let mut url = req.url.clone();
        if url.is_none() {
            loop {
                let num;
                loop {
                    let found = self.ports.choose(&mut rand::thread_rng());
                    let found_port = found.unwrap();
                    if port_is_available(*found_port) {
                        num = found_port.to_string();
                        break;
                    }
                }
                url = Some(format!("http://localhost:{}", num));

                let docker = Docker::new();
                let image = "actor1";

                let info = docker
                    .containers()
                    .create(&ContainerOptions::builder(image.as_ref()).extra_hosts(vec!["host.docker.internal:host-gateway"]).cmd(vec![num.to_string().as_str()]).expose(num.parse().unwrap(), "tcp", num.parse().unwrap()).build())
                    .await.unwrap();
                let result = docker.containers().get(info.id).start().await;
                if result.is_ok() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    break;
                }
            }
        }
        if req.url.is_none() {
            let mut try_count = 2;
            while try_count > 0 {
                let mut actor_service_client: ActorServiceClient<Channel>;
                let clients = self.clients.read().await;
                let url_test = url.clone().unwrap();
                if !clients.contains_key(url_test.as_str()) {
                    drop(clients);
                    let temp_client = ActorServiceClient::connect(url_test.clone()).await;
                    if temp_client.is_err() {
                        continue;
                    }
                    let value = temp_client.unwrap();
                    actor_service_client = value.clone();
                    let clients1000 = self.clients.clone();
                    tokio::spawn(async move {
                        let mut clients_write = clients1000.write().await;
                        clients_write.insert(url_test.clone(), value);
                        drop(clients_write);
                    });
                } else {
                    actor_service_client = clients.get(url.clone().unwrap().as_str()).unwrap().clone();
                    drop(clients);
                }

                let teq = Request::new(RunRequest {
                    context: req.context.clone(),
                    file: vec![],
                    function_id: req.id.clone(),
                });
                let response = actor_service_client.run(teq).await;
                try_count -= 1;
                if response.is_err() {
                    if try_count == 0 {
                        return Ok(Response::new(Status {
                            status: 2,
                        }));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                let res = response.unwrap().into_inner();
                return Ok(Response::new(Status {
                    status: res.response as i32,
                }));
            }
        } else {
            let mut actor_service_client;
            let clients = self.clients.read().await;

            if clients.contains_key(url.clone().unwrap().as_str()) {
                actor_service_client = clients.get(url.clone().unwrap().as_str()).unwrap().clone();
                drop(clients);
            } else {
                drop(clients);
                let client = ActorServiceClient::connect(url.clone().unwrap()).await.expect("CANNOT CONNECT TO ACTOR")
                    .max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000);
                let clients1000 = self.clients.clone();
                actor_service_client = client.clone();

                tokio::spawn(async move {
                    let mut clients_write = clients1000.write().await;
                    clients_write.insert(url.clone().unwrap(), client);
                    drop(clients_write);
                });
            }

            let run_request = Request::new(RunRequest {
                context: req.context,
                file: vec![],
                function_id: req.id.clone(),
            });
            let response = actor_service_client.new_execution(run_request).await;

            if response.is_err() {
                return Ok(Response::new(Status {
                    status: 2,
                }));
            }
            let res = response.unwrap().into_inner();
            return Ok(Response::new(Status {
                status: res.response as i32,
            }));
        }
        Ok(Response::new(Status {
            status: 1,
        }))
    }
}

struct FunctionEventSubscriber {
    ports: Vec<u16>,
    clients: Arc<RwLock<HashMap<String, ActorServiceClient<Channel>>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).expect("no port given");
    let bus_port = std::env::args().nth(2).expect("no bus port given");

    let addr = format!("[::0]:{}", port.clone());
    let id = Uuid::new_v4().to_string();

    println!("\x1b[92m Running Function Subscriber [{}][{}]\x1b[0m", id.clone(), addr.clone());
    let greeter = FunctionEventSubscriber {
        ports: (1025..65535).collect(),
        clients: Arc::new(RwLock::new(HashMap::new())),
    };

    let mut client =
        EventReceptionClient::connect(format!("http://localhost:{}", bus_port)).await.expect("Cannot connect to BUS Reception").max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000);
    client.subscribe(event_reception_service::Subscriber {
        topic: "function".to_string(),
        id: id.clone(),
        url: format!("http://localhost:{}", port.clone()).to_string(),
    }).await.expect("Bus not available");

    println!("\x1b[92m Subscribed to bus running at: [{}]\x1b[0m", format!("http://localhost:{}", bus_port));

    Server::builder()
        .add_service(EventSubscriberServiceServer::new(greeter).max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000))
        .serve(addr.parse()?)
        .await?;

    Ok(())
}
