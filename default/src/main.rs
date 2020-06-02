use lambda::handler_fn;
use serde_json::{json, Value};

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(default)).await?;
    Ok(())
}

async fn default(event: Value) -> Result<Value, Error> {
    println!("default {:#?}", event);
    // todo: something more appropriate
    Ok(json!({
        "statusCode": 400
    }))
}
