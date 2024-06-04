use std::{fs, thread};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::executor;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use wasmedge_sdk::{Caller, host_function, ImportObjectBuilder, Module, NeverType, params, Store, Vm, VmBuilder, WasmVal, WasmValue};
use wasmedge_sdk::config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions};
use wasmedge_sdk::error::HostFuncError;

use crate::actor_state::event_reception_service::{Event, InternalRequest, InternalResponse, RegisterRequest, UnregisterRequest};
use crate::actor_state::event_reception_service::event_reception_client::EventReceptionClient;
use crate::event_bus_service::Context;

pub mod event_reception_service {
    tonic::include_proto!("event_reception_service");
}

pub mod event_bus_service {
    tonic::include_proto!("event_bus_service");
}

pub struct ActorServer {
    pub actor: Arc<Mutex<FunctionActor>>,
    pub thread_park_channel: Arc<Mutex<(Sender<String>, Receiver<String>)>>,
    pub shutdown_channel: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    pub thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    pub will_wait: Arc<Mutex<bool>>,
    pub message: Arc<Mutex<Option<Context>>>,
}

pub struct FunctionActor {
    pub(crate) state: Arc<Mutex<Option<Box<dyn State + Send + Sync>>>>,
    pub stop: Arc<Mutex<bool>>,
    pub client: Arc<tokio::sync::Mutex<EventReceptionClient<Channel>>>,
    pub unpark_thread: Arc<Mutex<bool>>,
    pub new_request: Arc<Mutex<bool>>,
    pub vm: Arc<Mutex<Option<Vm>>>,
    pub thread_park_channel: Arc<Mutex<(Sender<String>, Receiver<String>)>>,
    pub will_wait: Arc<Mutex<bool>>,
    pub function_id: Option<String>,
    pub url: String,
    pub bus_url: String,
    pub(crate) message: Arc<Mutex<Option<Context>>>,
    pub(crate) response: Arc<Mutex<Option<Context>>>,
}


async fn grpc_call_function(input: &Vec<u8>, resource_id: &str) -> InternalResponse {
    let mut client;
    client =
        EventReceptionClient::connect("http://host.docker.internal:50058").await.expect("Cannot connect to BUS").max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000);
    let response = client.call_function_internally(InternalRequest {
        id: "".to_string(),
        context: input.to_vec(),
        resource_id: resource_id.to_string(),
        source_id: "".to_string(),
    }).await;

    return response.unwrap().into_inner();
}

#[host_function]
fn call_function(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let input_address = args[0].to_i32();
    let input_length = args[1].to_i32();
    let resource_id_address = args[2].to_i32();
    let resource_address = args[3].to_i32();
    let buffer_address = args[4].to_i32();
    let input_context = caller.memory(0).unwrap().read(input_address as u32, input_length as u32);
    let resource_id_context = caller.memory(0).unwrap().read(resource_id_address as u32, resource_address as u32);
    let input = input_context.unwrap();
    let resource_id = resource_id_context.unwrap();
    let resource_id_str = String::from_utf8_lossy(&resource_id);
    let response = executor::block_on(async {
        let response = grpc_call_function(input.as_ref(), &resource_id_str.as_ref()).await;
        return response;
    });
    let output: &Vec<u8> = response.context.as_ref();
    let output_length = output.len();
    caller.memory(0).unwrap().write(output, buffer_address as u32).expect("Updated");

    Ok(vec![WasmValue::from_i32(output_length as i32)])
}

#[host_function]
fn get_memory(_caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let result = args[0].to_i32();

    let mut data: Vec<u8> = Vec::with_capacity(result as usize);
    let pointer = data.as_mut_ptr();
    Ok(vec![WasmValue::from_i32(pointer as i32)])
}

#[async_trait(? Send)]
pub trait Actor {
    async fn next(&mut self);
    fn get(&self) -> &Self;
    async fn start(&mut self);
    async fn register_actor(&self);
    async fn unregister_actor(&self);
    fn create_vm(&mut self);
    fn wait(&self);
    fn run(&mut self);
    fn load_function(&self);
}

#[async_trait(? Send)]
impl Actor for FunctionActor {
    async fn next(&mut self) {
        let binding = self.state.clone();
        let mut guard = binding.lock().unwrap();
        if let Some(state) = guard.take() {
            let new_state = state.next(self).await;
            *guard = Some(new_state);
        }
    }

    fn get(&self) -> &Self {
        self
    }

    async fn start(&mut self) {
        loop {
            let stop = self.stop.clone().lock().unwrap().clone();
            if stop {
                break;
            }
            self.next().await;
        }
    }

    async fn register_actor(&self) {
        let client1 = self.client.lock().await;
        let mut client = client1.clone();

        client.register_warm_actor(RegisterRequest {
            function_id: self.function_id.clone().unwrap().to_string(),
            url: self.url.clone(),
        }).await.expect("Connection not available");
    }

    async fn unregister_actor(&self) {
        let client1 = self.client.lock().await;
        let mut client = client1.clone();

        let response = client.unregister_warm_actor(UnregisterRequest {
            function_id: self.function_id.clone().unwrap().to_string(),
            url: self.url.clone(),
        }).await;
        if response.is_err() {
            return;
        } else {
            response.unwrap();
            return;
        }
    }

    fn create_vm(&mut self) {
        let config_default = CommonConfigOptions::default();
        let config = ConfigBuilder::new(config_default)
            .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
            .build()
            .expect("Invalid Wasi Config");
        let mut vm = VmBuilder::new()
            .with_store(Store::new().unwrap())
            .with_config(config)
            .build()
            .expect("Configuration Not Found");
        vm.wasi_module_mut()
            .expect("Not found wasi module")
            .initialize(None, None, None);

        self.vm = Arc::new(Mutex::new(Some(vm)));
    }

    fn wait(&self) {
        let timeout = Duration::from_secs(300);
        let beginning_park = Instant::now();
        let mut timeout_remaining = timeout;

        loop {
            thread::park_timeout(timeout_remaining);
            let elapsed = beginning_park.elapsed();
            if elapsed >= timeout {
                let mut wait_lock = self.will_wait.lock().unwrap();
                if *wait_lock {
                    *wait_lock = false;
                    break;
                }
            }
            let channel = self.thread_park_channel.lock().unwrap();
            let (_, rx) = channel.deref();
            match rx.try_recv() {
                Ok(_) => {
                    let mut new_request_guard = self.new_request.lock().unwrap();
                    *new_request_guard = true;
                    let mut wait_lock = self.will_wait.lock().unwrap();
                    *wait_lock = false;
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    println!("DISCONNECTED MESSAGE CHANNEL");
                    break;
                }
                Err(TryRecvError::Empty) => {
                    println!("EMPTY MESSAGE CHANNEL")
                }
            }
            timeout_remaining = timeout - elapsed;
        }
    }

    fn run(&mut self) {
        let vm_arc = self.vm.lock().unwrap();

        let mut vm = vm_arc.clone().unwrap();
        let message_lock = self.message.lock().unwrap();
        let message = message_lock.clone().unwrap();
        drop(message_lock);
        let url_struct = message.input.unwrap();
        let url_len = url_struct.len();
        let res = vm.run_func(Some("modul1"), "get_memory1", params![url_len as i32]).expect("--");
        vm.named_module("modul1").expect("Not known").memory("memory").unwrap().write(url_struct, res[0].to_i32() as u32).unwrap();

        let run_response = vm.run_func(Some("modul1"), "run", params![res[0].to_i32() as i32, url_len as i32]).expect("---");
        let buffer = vm.run_func(Some("modul1"), "get_size", params![]).expect("---");
        let buffer_content = vm.named_module("modul1").expect("Not known").memory("memory").unwrap().read(run_response[0].to_i32() as u32, buffer[0].to_i32() as u32).unwrap();

        let mut response_lock = self.response.lock().unwrap();
        *response_lock = Some(Context {
            id: message.id,
            output: Some(buffer_content),
            bus_url: message.bus_url,
            workflow_id: message.workflow_id,
            step: message.step,
            internal: message.internal,
            request_id: message.request_id,
            input: None,
        });
    }

    fn load_function(&self) {
        let mut vim_arc = self.vm.lock().unwrap();
        let mut vm = vim_arc.clone().unwrap();
        let import = ImportObjectBuilder::new()
            .with_func::<(i32, i32, i32, i32, i32), i32, NeverType>("call_function", call_function, None).expect("--")
            .with_func::<(i32), (i32), NeverType>("get_memory", get_memory, None).expect("--")
            .build::<NeverType>("env", None).expect("..");
        vm.register_import_module(&import).expect("sdas");

        let file = self.function_id.clone().unwrap().clone() + ".wasm";
        let contents = fs::read(file.clone())
            .expect("Should have been able to read the file");
        let module = Module::from_bytes(None, contents)
            .expect("Invalid Input");
        vm = vm.clone()
            .register_module(Some("modul1"), module)
            .expect("Module Not Found");
        *vim_arc = Some(vm);
    }
}


pub struct Created;

pub struct Started;

pub struct Running;

pub struct Finished;

pub struct AfterWaiting;

pub struct Waiting;

pub struct Stopped;

pub enum CurrentState {
    CREATED,
    STARTED,
    RUNNING,
    FINISHED,
    WAITING,
    STOPPED,
}

#[async_trait(? Send)]
pub trait State {
    fn current_state(self: Box<Self>) -> CurrentState;
    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync>;
}

#[async_trait(? Send)]
impl State for Created {
    fn current_state(self: Box<Self>) -> CurrentState {
        CurrentState::CREATED
    }

    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync> {
        actor.create_vm();
        Box::new(Started)
    }
}

#[async_trait(? Send)]
impl State for Started {
    fn current_state(self: Box<Self>) -> CurrentState {
        CurrentState::STARTED
    }

    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync> {
        actor.load_function();
        Box::new(Running)
    }
}

#[async_trait(? Send)]
impl State for Running {
    fn current_state(self: Box<Self>) -> CurrentState {
        CurrentState::RUNNING
    }

    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync> {
        actor.run();
        Box::new(Finished)
    }
}

#[async_trait(? Send)]
impl State for Finished {
    fn current_state(self: Box<Self>) -> CurrentState {
        CurrentState::FINISHED
    }

    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync> {
        let client1 = actor.client.lock().await;
        let mut client = client1.clone();

        let message_lock = actor.response.lock().unwrap();
        let message = message_lock.clone();

        client.publish(Event {
            topic: "result".to_string(),
            resource_id: actor.function_id.clone().unwrap(),
            context: message,
        }).await.expect("Bus Not Available");
        let mut wait_lock = actor.will_wait.lock().unwrap();
        *wait_lock = true;
        Box::new(Waiting)
    }
}

#[async_trait(? Send)]
impl State for Waiting {
    fn current_state(self: Box<Self>) -> CurrentState {
        CurrentState::WAITING
    }

    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync> {
        actor.register_actor().await;
        actor.wait();
        let mut new_request_guard = actor.new_request.lock().unwrap();
        if *new_request_guard {
            *new_request_guard = false;
            return Box::new(Running);
        }
        Box::new(Stopped)
    }
}

#[async_trait(? Send)]
impl State for Stopped {
    fn current_state(self: Box<Self>) -> CurrentState {
        CurrentState::STOPPED
    }

    async fn next(self: Box<Self>, actor: &mut FunctionActor) -> Box<dyn State + Send + Sync> {
        actor.unregister_actor().await;
        let mut value = actor.stop.lock().unwrap();
        *value = true;
        Box::new(Stopped)
    }
}