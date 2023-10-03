use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Response<R> {
    result: R,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReverseRequest {
    value: String,
}

pub fn reverse(req: ReverseRequest) -> Response<String> {
    Response {
        result: req.value.chars().rev().collect(),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CalcRequest {
    x: i32,
    y: i32,
}

pub fn add(req: CalcRequest) -> Response<i32> {
    Response {
        result: req.x + req.y,
    }
}

pub fn sub(req: CalcRequest) -> Response<i32> {
    Response {
        result: req.x - req.y,
    }
}
