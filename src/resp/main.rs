mod handler;
mod logic;

use handler::*;
use logic::*;

use mqtt::{Message, MessageBuilder, Receiver};
use std::collections::HashMap;
use std::{env, error::Error, io, process, thread, time::Duration};

use crossbeam_channel::bounded;

extern crate paho_mqtt as mqtt;

const DEFAULT_BROKER: &str = "tcp://localhost:1883";
const DEFAULT_CLIENT: &str = "rust_responder";
const DEFAULT_QOS: i32 = 1;

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

fn subscribe(cli: &mqtt::Client, handlers: &MessageHandlersMap) -> Result<(), Box<dyn Error>> {
    for t in handlers.keys() {
        cli.subscribe(t, DEFAULT_QOS)?;
    }
    Ok(())
}

fn handlers_map() -> MessageHandlersMap {
    let mut m: MessageHandlersMap = HashMap::new();
    m.insert(String::from("rust/reverse"), responding_handler(reverse));
    m.insert(String::from("rust/add"), responding_handler(add));
    m.insert(String::from("rust/sub"), responding_handler(sub));
    m
}

fn wait_for_enter_pressed() -> io::Result<()> {
    // there's probably a more elegant way, but this will do for now:
    let stdin = io::stdin();
    let mut buf = String::new();
    stdin.read_line(&mut buf).map(|_|{()})
}

fn main() {
    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_BROKER.to_string());

    // Create a client.
    let client = {
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
    let rx: Receiver<Option<Message>> = client.start_consuming();

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
    if let Err(e) = client.connect(conn_opts) {
        println!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }

    let (s_abort, r_abort) = bounded(1);

    let join_handler = thread::spawn(move || {
        let handlers = handlers_map();
        let fallback_handler = no_such_topic_handler();

        if let Err(e) = subscribe(&client, &handlers) {
            println!("Error subscribing topics: {:?}", e);
            process::exit(1);
        }
        println!("Processing requests...");

        while let Err(_) = r_abort.try_recv() {
            let msg = rx.recv_timeout(Duration::from_millis(500));
            if let Ok(msg_opt) = msg {
                if let Some(msg) = msg_opt {
                    let handler: &MessageHandler =
                        handlers.get(msg.topic()).unwrap_or(&fallback_handler);
                    if let Err(e) = handler(&client, &msg) {
                        println!("Error handling message on topic {:?}: {:?}", msg.topic(), e);
                    }
                } else if !client.is_connected() {
                    if try_reconnect(&client) {
                        println!("Resubscribe topics...");
                        if let Err(e) = subscribe(&client, &handlers) {
                            println!("Error subscribing topics: {:?}", e);
                            process::exit(1);
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        // If still connected, then disconnect now.
        if client.is_connected() {
            println!("Disconnecting");
            handlers
                .keys()
                .try_fold((), |_acc, topic| client.unsubscribe(topic))
                .unwrap();
            client.disconnect(None).unwrap();
        }
    });

    wait_for_enter_pressed().unwrap();
    s_abort.send(()).unwrap();

    join_handler.join().unwrap();

    println!("Exiting");
}
