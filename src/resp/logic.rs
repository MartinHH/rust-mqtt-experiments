use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct ReverseRequest {
    value: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StringResponse {
    result: String,
}

pub fn reverse(req: ReverseRequest) -> StringResponse {
    StringResponse {
        result: req.value.chars().rev().collect(),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CalcRequest {
    x: i32,
    y: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CalcResponse {
    result: i32,
}

pub fn add(req: CalcRequest) -> CalcResponse {
    CalcResponse {
        result: req.x + req.y,
    }
}

pub fn sub(req: CalcRequest) -> CalcResponse {
    CalcResponse {
        result: req.x - req.y,
    }
}
