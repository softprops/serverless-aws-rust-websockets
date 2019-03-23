use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_json::{json, Value};

fn main() {
    lambda!(handler)
}

fn handler(
    event: Value,
    _: Context,
) -> Result<Value, HandlerError> {
    println!("default {:#?}", event);
    // todo: something more appropriate
    Ok(json!({
        "statusCode": 400
    }))
}
