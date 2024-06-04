#![macro_use]

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use futures::lock::Mutex;
use rand::prelude::IteratorRandom;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::event_bus_service;
use crate::event_bus_service::Context;
use crate::event_subscriber_service::event_subscriber_service_client::EventSubscriberServiceClient;
use crate::event_subscriber_service::Message;

pub mod actor_service {
    tonic::include_proto!("actor_service");
}

#[derive(Debug)]
pub struct EventStruct {
    pub(crate) topic: String,
    pub(crate) context: Option<Context>,
    pub(crate) resource_id: String,
}

#[derive(Debug, Clone)]
pub struct EventStore {
    topic: String,
    context: Option<Context>,
    resource_id: String,
}

#[derive(Debug, Clone)]
pub struct WarmFunction {
    pub(crate) url: String,
    pub(crate) trying: Arc<Mutex<bool>>,
}

impl Hash for WarmFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.url.hash(state)
    }
}

impl Eq for WarmFunction {}

impl PartialEq for WarmFunction {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
    }

    fn ne(&self, other: &Self) -> bool {
        self.url != other.url
    }
}

#[derive(Debug, Clone)]
pub struct EventSubscriber {
    pub(crate) url: String,
    pub(crate) topic: String,
    pub(crate) client: Option<EventSubscriberServiceClient<Channel>>,
}

impl Clone for EventStruct {
    fn clone(&self) -> Self {
        EventStruct {
            topic: self.topic.clone(),
            context: self.context.clone(),
            resource_id: self.resource_id.clone(),
        }
    }
}

#[tonic::async_trait]
pub trait EventBus {
    async fn publish_event(&self, event: &EventStruct);
    async fn subscribe_event(&self, subscriber: &EventSubscriber);
    async fn get_topic_sub_count(&self, topic: &str) -> usize;
    async fn register_function(&self, function_id: String, url: &str) -> usize;
    async fn unregister_function(&self, function_id: String, url: &str) -> usize;
    async fn get_warm_function(&self, function_id: &str) -> Option<String>;
}

pub struct ServerlessEventBus {
    pub(crate) events_sender: (UnboundedSender<EventStore>),
    pub(crate) events: Arc<RwLock<HashMap<String, Vec<EventStore>>>>,
    pub(crate) subscribers: Arc<RwLock<HashMap<String, Vec<EventSubscriber>>>>,
    pub(crate) warm_functions: Arc<RwLock<HashMap<String, Mutex<VecDeque<WarmFunction>>>>>,
}

pub(crate) async fn run_subscriber(subscribers_clone: Arc<RwLock<HashMap<String, Vec<EventSubscriber>>>>,
                                   new_event_clone: EventStore,
                                   warm_functions: Arc<RwLock<HashMap<String, Mutex<VecDeque<WarmFunction>>>>>) {
    let subscribers_lock = subscribers_clone.read().await;
    if subscribers_lock.contains_key(new_event_clone.topic.as_str()) {
        let topic_subscribers = subscribers_lock.get(new_event_clone.topic.as_str()).unwrap().clone();
        for mut topic_subscriber in topic_subscribers {
            let subscriber_url = topic_subscriber.url.clone();
            let mut found = false;
            let mut client;
            if topic_subscriber.client.is_none() {
                topic_subscriber.client = Some(
                    EventSubscriberServiceClient::connect(subscriber_url.clone()).await.expect("Cannot connect to Subscriber").max_decoding_message_size(100 * 1000000).max_encoding_message_size(100 * 1000000));
                client = topic_subscriber.client.as_ref().unwrap().clone();
            } else {
                client = topic_subscriber.client.as_ref().unwrap().clone();
            }
            let mut total_tries = 3;
            if new_event_clone.topic == "function" {
                loop {
                    let warm_actors_lock = warm_functions.read().await;
                    let functions = warm_actors_lock.get(new_event_clone.resource_id.as_str());
                    if functions.is_some() {
                        let mut functions_values = functions.unwrap().lock().await;
                        if !functions_values.is_empty() {
                            if total_tries <= 0 {
                                break;
                            }
                            total_tries -= 1;
                            let fun = functions_values.pop_front().unwrap();
                            drop(functions_values);
                            let url = Some(fun.url.clone());
                            let response = client.notify(Message {
                                id: new_event_clone.resource_id.clone(),
                                context: new_event_clone.context.clone(),
                                url: url.clone(),
                            }).await.expect("Cannot notify subscriber");
                            let res = response.into_inner();
                            if res.status == 1 {
                                found = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            if !found {
                client.notify(Message {
                    id: new_event_clone.resource_id.clone(),
                    context: new_event_clone.context.clone(),
                    url: None,
                }).await.expect("Cannot notify subscriber");
            }
            break;
        }
    }
}

async fn watch_events(execution_id: String, events_clone: Arc<RwLock<HashMap<String, Vec<EventStore>>>>,
                      subscribers: Arc<RwLock<HashMap<String, Vec<EventSubscriber>>>>,
                      warm_functions: Arc<RwLock<HashMap<String, Mutex<VecDeque<WarmFunction>>>>>) {
    let mut events_count = 0;
    loop {
        let events_lock = events_clone.read().await;
        let subscribers_clone = subscribers.clone();
        let warm_functions_clone = warm_functions.clone();
        let available = events_lock.get(execution_id.as_str()).unwrap();
        if available.len() > events_count {
            let event_vector = available;
            events_count = event_vector.len();
            let new_event_clone = event_vector.last().unwrap().clone();
            tokio::spawn(async move {
                run_subscriber(subscribers_clone, new_event_clone, warm_functions_clone).await
            });
        }
    }
}

#[tonic::async_trait]
impl EventBus for ServerlessEventBus {
    async fn publish_event(&self, event: &EventStruct) {
        let channel = self.events_sender.clone();
        let event_clone = event.clone();
        tokio::spawn(async move {
            let mut context = event_clone.context.unwrap();
            if context.id.is_none() {
                context.id = Some(Uuid::new_v4().to_string());
            }

            let event_store = EventStore {
                resource_id: event_clone.resource_id.clone(),
                context: Some(context),
                topic: event_clone.topic.clone(),
            };

            channel.send(event_store.clone()).expect("Channel not available");
        });
    }


    async fn subscribe_event(&self, subscriber: &EventSubscriber) {
        let mut subscribers = self.subscribers.write().await;
        let subs_avail = subscribers.contains_key(subscriber.topic.as_str());
        if !subs_avail {
            subscribers.insert(subscriber.topic.to_string(), vec![]);
        }

        subscribers
            .get_mut(subscriber.topic.as_str())
            .unwrap()
            .push(subscriber.clone());
    }

    async fn get_topic_sub_count(&self, topic: &str) -> usize {
        let subscribers = self.subscribers.read().await;
        let subs_avail = subscribers.contains_key(topic);
        if !subs_avail {
            return 0;
        }

        let count = subscribers
            .get(topic)
            .unwrap()
            .len();
        count
    }

    async fn register_function(&self, function_id: String, url: &str) -> usize {
        let warm_function_lock1 = self.warm_functions.clone();
        let url_clone = url.to_string();
        tokio::task::spawn(async move {
            let mut warm_function_lock = warm_function_lock1.read().await;
            let warm_functions_list = warm_function_lock.get(function_id.as_str());
            return if warm_functions_list.is_none() {
                drop(warm_function_lock);
                let mut set = VecDeque::new();
                set.push_back(WarmFunction {
                    url: url_clone.clone(),
                    trying: Arc::new(Mutex::new(false)),
                });
                let mut warm_function_lock = warm_function_lock1.write().await;
                warm_function_lock.insert(function_id, Mutex::new(set));
                1
            } else {
                match warm_functions_list {
                    Some(wasm_functions) => {
                        let mut wasm_lock = wasm_functions.lock().await;
                        wasm_lock.push_back(WarmFunction {
                            url: url_clone.clone(),
                            trying: Arc::new(Mutex::new(false)),
                        });
                    }
                    _ => {}
                }
                1
            };
        });
        1
    }

    async fn unregister_function(&self, function_id: String, url: &str) -> usize {
        let url_clone = url.to_string();

        let warm_function_lock1 = self.warm_functions.clone();
        tokio::task::spawn(async move {
            let warm_function_lock = warm_function_lock1.read().await;
            let warm_functions_list = warm_function_lock.get(function_id.as_str());

            return if warm_functions_list.is_none() {
                2
            } else {
                match warm_functions_list {
                    Some(wasm_functions) => {
                        let mut wasm_lock = wasm_functions.lock().await;
                        let found = wasm_lock.iter().position((|x| x.url == url_clone));
                        if (found.is_some()) {
                            wasm_lock.remove(found.unwrap());
                        }
                    }
                    _ => {}
                }
                1
            };
        });
        1
    }

    async fn get_warm_function(&self, function_id: &str) -> Option<String> {
        let warm_actors_lock = self.warm_functions.read().await;
        let functions = warm_actors_lock.get(function_id);
        let mut url = None;
        if functions.is_some() {
            let ready_func = functions.unwrap().lock().await;
            if ready_func.len() > 0 {
                url = Some(ready_func.iter().next().unwrap().url.clone());
            }
        }
        return url;
    }
}

