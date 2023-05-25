use mqtt::{Message, MessageBuilder, PropertyCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

extern crate paho_mqtt as mqtt;

#[derive(Debug, Clone)]
pub enum MsgHandlingError {
    /// Error type for "received a request without a response-topic"
    NoResponseTopicError,
    /// Error "received a message for a topic that no handler is registered for"
    NoHandlerForTopicError,
}

impl fmt::Display for MsgHandlingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MsgHandlingError::NoResponseTopicError => write!(f, "No response topic was provided"),
            MsgHandlingError::NoHandlerForTopicError => {
                write!(f, "No handler was registered for this topic")
            }
        }
    }
}

impl Error for MsgHandlingError {}

pub type MessageHandler = Box<dyn Fn(&mqtt::Client, &Message) -> Result<(), Box<dyn Error>>>;

pub type MessageHandlersMap<'a> = HashMap<&'a str, MessageHandler>;

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

/// Generic request-resp.handler.
///
/// Extracts the payload from `req`, passed it to the `logic` and publishes the returned `O` as
/// response.
fn respond_generic<I: for<'de> Deserialize<'de>, O: Serialize, F: Fn(I) -> O>(
    cli: &mqtt::Client,
    req: &Message,
    logic: F,
) -> Result<(), Box<dyn Error>> {
    let topic = req
        .properties()
        .get_string(PropertyCode::ResponseTopic)
        .ok_or(MsgHandlingError::NoResponseTopicError)?;
    let request: I = serde_json::from_str(&req.payload_str())?;
    let response_payload = serde_json::to_string(&logic(request))?;
    let props = response_properties(req)?;
    let resp = MessageBuilder::new()
        .topic(topic.clone())
        .payload(response_payload)
        .qos(req.qos())
        .properties(props)
        .finalize();
    cli.publish(resp)?;

    Ok(())
}

pub fn responding_handler<
    I: for<'de> Deserialize<'de> + 'static,
    O: Serialize + 'static,
    F: Fn(I) -> O + Copy + 'static,
>(
    logic: F,
) -> MessageHandler {
    Box::new(move |cli, req| respond_generic(cli, req, logic))
}

pub fn no_such_topic_handler() -> MessageHandler {
    Box::new(|_cli, _req| Err(MsgHandlingError::NoHandlerForTopicError.into()))
}
