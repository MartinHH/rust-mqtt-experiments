mod logic;

use logic::*;
use mqtt::{Message, MessageBuilder, PropertyCode, Receiver};
use serde::{Deserialize, Serialize};
use std::{env, error::Error, fmt, process, thread, time::Duration};

extern crate paho_mqtt as mqtt;

const DEFAULT_BROKER: &str = "tcp://localhost:1883";
const DEFAULT_CLIENT: &str = "rust_responder";
const REQ_TOPIC: &str = &"rust/req";
const NOTIFY_TOPIC: &str = &"rust/notify";
const DEFAULT_QOS: i32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub enum Notification {
    Responded { topic: String, count: usize },
    Ignored { error: String, count: usize },
}

/// Error type for "received a request without a response-topic"
#[derive(Debug, Clone)]
struct NoResponseTopicError;

impl fmt::Display for NoResponseTopicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "No response topic was provided")
    }
}

impl Error for NoResponseTopicError {}

// Reconnect to the broker when connection is lost.
fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("Connection lost. Waiting to retry connection");
    for _ in 0..12 {
        thread::sleep(Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

fn subscribe_request_topic(cli: &mqtt::Client) {
    if let Err(e) = cli.subscribe(REQ_TOPIC, DEFAULT_QOS) {
        println!("Error subscribing topic: {:?}", e);
        process::exit(1);
    }
}

fn publish_notification(
    cli: &mqtt::Client,
    notification: &Notification,
) -> Result<(), Box<dyn Error>> {
    let payload = serde_json::to_string(notification)?;
    let resp = MessageBuilder::new()
        .topic(NOTIFY_TOPIC)
        .payload(payload)
        .qos(DEFAULT_QOS)
        .finalize();
    cli.publish(resp).map_err(|e| e.into())
}

fn response_properties(msg: &Message) -> mqtt::errors::Result<mqtt::Properties> {
    if let Some(cd) = msg.properties().get_binary(PropertyCode::CorrelationData) {
        let mut props = mqtt::Properties::new();
        props
            .push_binary(PropertyCode::CorrelationData, cd)
            .map(|_| props)
    } else {
        Ok(mqtt::Properties::new())
    }
}

/// Generic request-handler.
///
/// Extracts the payload from `req`, passed it to the `logic` and publishes the returned `O` as
/// response.
fn respond_generic<I: for<'de> Deserialize<'de>, O: Serialize, F: Fn(I) -> O>(
    cli: &mqtt::Client,
    req: &Message,
    logic: F,
) -> Result<String, Box<dyn Error>> {
    let topic = req
        .properties()
        .get_string(PropertyCode::ResponseTopic)
        .ok_or(NoResponseTopicError)?;
    let request: I = serde_json::from_str(&req.payload_str())?;
    let response_payload = serde_json::to_string(&logic(request))?;
    let props = response_properties(req)?;
    let resp = MessageBuilder::new()
        .topic(topic.clone())
        .payload(response_payload)
        .qos(DEFAULT_QOS)
        .properties(props)
        .finalize();
    cli.publish(resp)?;

    Ok(topic)
}

fn main() {
    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_BROKER.to_string());

    // Create a client.
    let cli = {
        // Define the set of options for the create.
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
            .mqtt_version(mqtt::MQTT_VERSION_5)
            .client_id(DEFAULT_CLIENT.to_string())
            .finalize();

        mqtt::Client::new(create_opts).unwrap_or_else(|err| {
            println!("Error creating the client: {:?}", err);
            process::exit(1);
        })
    };

    // Initialize the consumer before connecting.
    let rx: Receiver<Option<Message>> = cli.start_consuming();

    // Define the set of options for the connection.
    let conn_opts = {
        let lwt = MessageBuilder::new()
            .topic("rust/connection/lost")
            .payload("Responder disconnected ungracefully")
            .finalize();
        mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(false)
            .will_message(lwt)
            .finalize()
    };

    // Connect and wait for it to complete or fail.
    if let Err(e) = cli.connect(conn_opts) {
        println!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }

    // Subscribe request-topic.
    subscribe_request_topic(&cli);

    println!("Processing requests...");
    for (i, msg) in rx.iter().enumerate() {
        if let Some(msg) = msg {
            let notification = respond_generic(&cli, &msg, handle_request)
                .map(|t| Notification::Responded { topic: t, count: i })
                .unwrap_or_else(|e| Notification::Ignored {
                    error: format!("{}", e),
                    count: i,
                });
            if let Err(e) = publish_notification(&cli, &notification) {
                println!(
                    "Failed to publish this Notification: {:?}\n\tError: {:?}",
                    notification, e
                );
            }
        } else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("Resubscribe topics...");
                subscribe_request_topic(&cli);
            } else {
                break;
            }
        }
    }

    // If still connected, then disconnect now.
    if cli.is_connected() {
        println!("Disconnecting");
        cli.unsubscribe(REQ_TOPIC).unwrap();
        cli.disconnect(None).unwrap();
    }
    println!("Exiting");
}
