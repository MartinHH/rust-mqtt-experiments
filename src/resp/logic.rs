use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Request {
    value: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    result: String,
}

pub fn handle_request(req: Request) -> Response {
    Response {
        result: req.value.chars().rev().collect(),
    }
}
