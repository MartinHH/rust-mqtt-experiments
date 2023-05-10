use std::{env, process, thread, time::Duration};
use mqtt::{MessageBuilder, PropertyCode};

extern crate paho_mqtt as mqtt;

const DFLT_BROKER: &str = "tcp://localhost:1883";
const DFLT_CLIENT: &str = "rust_responder";
const DFLT_TOPIC: &str = &"rust/req";
const DFLT_QOS: i32 = 1;

// Reconnect to the broker when connection is lost.
fn try_reconnect(cli: &mqtt::Client) -> bool
{
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

fn subscribe_topic(cli: &mqtt::Client) {
    if let Err(e) = cli.subscribe(DFLT_TOPIC, DFLT_QOS) {
        println!("Error subscribing topic: {:?}", e);
        process::exit(1);
    }
}

fn main() {
    let host = env::args().nth(1).unwrap_or_else(||
        DFLT_BROKER.to_string()
    );

    // Define the set of options for the create.
    // Use an ID for a persistent session.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .client_id(DFLT_CLIENT.to_string())
        .finalize();

    // Create a client.
    let cli = mqtt::Client::new(create_opts).unwrap_or_else(|err| {
        println!("Error creating the client: {:?}", err);
        process::exit(1);
    });

    // Initialize the consumer before connecting.
    let rx = cli.start_consuming();

    // Define the set of options for the connection.
    let lwt = MessageBuilder::new()
        .topic("test")
        .payload("Responder lost connection")
        .finalize();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(false)
        .will_message(lwt)
        .finalize();

    // Connect and wait for it to complete or fail.
    if let Err(e) = cli.connect(conn_opts) {
        println!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }

    // Subscribe request-topic.
    subscribe_topic(&cli);

    println!("Processing requests...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            if let Some(topic) = msg.properties()
                .get_string(PropertyCode::ResponseTopic) {
                let mut props = mqtt::Properties::new();
                if let Some(cd) = msg.properties()
                    .get_binary(PropertyCode::CorrelationData) {
                    props.push_binary(PropertyCode::CorrelationData, cd.clone()).unwrap();
                }
                let resp =
                    MessageBuilder::new()
                        .topic(topic)
                        .payload(msg.payload())
                        .qos(DFLT_QOS)
                        .properties(props)
                        .finalize();
                cli.publish(resp).unwrap();
            } else {
                println!("No Response topic - ignoring request");
            }
        } else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("Resubscribe topics...");
                subscribe_topic(&cli);
            } else {
                break;
            }
        }
    }

    // If still connected, then disconnect now.
    if cli.is_connected() {
        println!("Disconnecting");
        cli.unsubscribe(DFLT_TOPIC).unwrap();
        cli.disconnect(None).unwrap();
    }
    println!("Exiting");
}
