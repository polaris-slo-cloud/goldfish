use std::ops::Deref;
use std::sync::{Arc, mpsc, Mutex};
use std::thread;

use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use crate::actor_service::{GetStatus, RunRequest, RunResponse};
use crate::actor_service::actor_service_server::{ActorService, ActorServiceServer};
use crate::actor_state::{Actor, ActorServer, Created, FunctionActor};
use crate::actor_state::event_bus_service;
use crate::actor_state::event_reception_service::event_reception_client::EventReceptionClient;

mod actor_state;

pub mod actor_service {
    tonic::include_proto!("actor_service");
}

#[tonic::async_trait]
impl ActorService for ActorServer {
    async fn run(&self, request: Request<RunRequest>) -> Result<Response<RunResponse>, Status> {
        let actor_clone = self.actor.clone();
        let will_wait_clone = self.will_wait.clone();
        let thread_clone = self.thread.clone();
        let shutdown_channel_clone = self.shutdown_channel.clone();
        let mut actor_thread = thread_clone.lock().unwrap();
        let req = request.into_inner().clone();
        let message = req.context.unwrap();

        let new_message_lock = self.message.clone();
        let mut new_message = new_message_lock.lock().unwrap();
        *new_message = Some(message);
        drop(new_message);

        if actor_thread.is_some() {
            return Ok(Response::new(RunResponse {
                response: 2
            }));
        }

        let mut actor_function_id = actor_clone.lock().unwrap();
        actor_function_id.function_id = Some(req.function_id);
        drop(actor_function_id);

        *actor_thread = Some(thread::spawn(move || {
            loop {
                let mut actor = actor_clone.lock().unwrap();
                let mut message_lock = actor.message.lock().unwrap();
                let new_input_lock = new_message_lock.lock().unwrap();
                let new_input = new_input_lock.clone().unwrap();
                *message_lock = Some(new_input);
                drop(message_lock);
                drop(new_input_lock);
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(actor.next());
                let actor_will_wait_lock = actor.will_wait.lock().unwrap();
                let mut will_wait_value = will_wait_clone.lock().unwrap();
                *will_wait_value = *actor_will_wait_lock;
                let mut actor_stop = actor.stop.lock().unwrap();
                if *actor_stop {
                    *actor_stop = false;
                    break;
                }
            }
            if let Some(shutdown_channel_sender) = shutdown_channel_clone.lock().unwrap().take() {
                shutdown_channel_sender.send(()).unwrap();
            }
        }));
        Ok(Response::new(RunResponse {
            response: 1
        }))
    }

    async fn new_execution(&self, request: Request<RunRequest>) -> Result<Response<RunResponse>, Status> {
        let will_wait_clone = self.will_wait.clone();
        let mut lock = will_wait_clone.try_lock();
        if let Ok(ref mut mutex) = lock {
            if **mutex == false {
                return Ok(Response::new(RunResponse {
                    response: 2
                }));
            }
            **mutex = false;
            let req = request.into_inner();
            let message = req.context.unwrap();

            let message_lock = self.message.clone();
            let message_lock_available = message_lock.try_lock();

            if message_lock_available.is_err() {
                return Ok(Response::new(RunResponse {
                    response: 2
                }));
            }
            let mut new_message_lock = message_lock_available.unwrap();
            *new_message_lock = Some(message);
            let thread_clone = self.thread.clone();
            let thread_lock_available = thread_clone.try_lock();
            if thread_lock_available.is_err() {
                return Ok(Response::new(RunResponse {
                    response: 2
                }));
            }
            let mut actor_thread = thread_lock_available.unwrap();
            let channel = self.thread_park_channel.lock().unwrap();
            let (unblock_sender, _) = channel.deref();
            unblock_sender.send("unblock".to_string()).expect("Channel not available");
            drop(channel);
            actor_thread.as_mut().unwrap().thread().unpark();
            Ok(Response::new(RunResponse {
                response: 1
            }))
        } else {
            return Ok(Response::new(RunResponse {
                response: 2
            }));
        }
    }

    async fn get_status(&self, _request: Request<GetStatus>) -> Result<Response<RunResponse>, Status> {
        return match self.will_wait.clone().try_lock() {
            Ok(_) => Ok(Response::new(RunResponse {
                response: 1
            })),
            Err(_) => Ok(Response::new(RunResponse {
                response: 2
            }))
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).expect("no bus given");

    let thread_park_channel = Arc::new(Mutex::new(mpsc::channel()));
    let (shutdown_channel_sender, shutdown_channel_receiver) = oneshot::channel::<()>();

    let actor =
        ActorServer {
            actor: Arc::new(Mutex::new(FunctionActor {
                client: Arc::new(tokio::sync::Mutex::new(EventReceptionClient::connect("http://host.docker.internal:50058").await.expect("Cannot connect to BUS"))),
                state: Arc::new(Mutex::new(Some(Box::new(Created)))),
                stop: Arc::new(Mutex::new(false)),
                unpark_thread: Arc::new(Mutex::new(false)),
                new_request: Arc::new(Mutex::new(false)),
                vm: Arc::new(Mutex::new(None)),
                thread_park_channel: Arc::clone(&thread_park_channel),
                will_wait: Arc::new(Mutex::new(false)),
                function_id: None,
                url: "http://localhost:".to_string() + port.as_str(),
                bus_url: "".to_string(),
                message: Arc::new(Mutex::new(None)),
                response: Arc::new(Mutex::new(None)),
            })),
            thread_park_channel: Arc::clone(&thread_park_channel),
            shutdown_channel: Arc::new(Mutex::new(Some(shutdown_channel_sender))),
            thread: Arc::new(Mutex::new(None)),
            will_wait: Arc::new(Mutex::new(false)),
            message: Arc::new(Mutex::new(None)),
        };

    let addr = format!("[::0]:{}", port).parse()?;

    Server::builder()
        .add_service(ActorServiceServer::new(actor).max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000))
        .serve_with_shutdown(addr, async {
            shutdown_channel_receiver.await.ok();
        })
        .await?;

    Ok(())
}