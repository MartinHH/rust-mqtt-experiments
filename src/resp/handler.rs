use mqtt::{Message, MessageBuilder, PropertyCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

extern crate paho_mqtt as mqtt;

pub type MessageHandler = Box<dyn Fn(&mqtt::Client, &Message) -> Result<(), Box<dyn Error>>>;

pub type MessageHandlersMap = HashMap<String, MessageHandler>;

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

/// fallback handler for unsupported topics
pub fn no_such_topic_handler() -> MessageHandler {
    Box::new(|_cli, _req| Err(MsgHandlingError::NoHandlerForTopicError.into()))
}

/// Converts a generic function to a request/response `MessageHandler`.
///
/// This is meant to decouple generic buisness logic from the mqtt client.
///
pub fn responding_handler<
    In: for<'de> Deserialize<'de> + 'static,
    Out: Serialize + 'static,
    F: Fn(In) -> Out + Copy + 'static,
>(
    logic: F,
) -> MessageHandler {
    Box::new(move |cli, req| respond_generic(cli, req, logic))
}

/// Generic request-resp.handler.
///
/// Extracts the payload from `req`, passes it to the `logic` and publishes the returned `Out` as
/// response.
fn respond_generic<In: for<'de> Deserialize<'de>, Out: Serialize, F: Fn(In) -> Out>(
    cli: &mqtt::Client,
    req: &Message,
    logic: F,
) -> Result<(), Box<dyn Error>> {
    let topic = req
        .properties()
        .get_string(PropertyCode::ResponseTopic)
        .ok_or(MsgHandlingError::NoResponseTopicError)?;
    let request: In = serde_json::from_str(&req.payload_str())?;
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

/// Factory for mqtt properties of the responses.
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
