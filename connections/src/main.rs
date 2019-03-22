use dynomite::{
    dynamodb::{DeleteItemInput, DynamoDb, DynamoDbClient, PutItemInput},
    Item,
};
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;

thread_local!(
    static DDB: DynamoDbClient = DynamoDbClient::new(Default::default());
);

#[derive(Item)]
struct Connection {
    #[hash]
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    request_context: RequestContext,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestContext {
    event_type: EventType,
    connection_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum EventType {
    Connect,
    Disconnect,
}

fn main() {
    lambda!(handler)
}

fn handler(event: Event, _: Context) -> Result<Value, HandlerError> {
    let connection = Connection {
        id: event.request_context.connection_id,
    };
    let table_name = match env::var("tableName") {
        Ok(table_name) => table_name,
        _ => {
            return Ok(json!({
                "statusCode": 500,
                "body": "failed to resolve a db table"
            }))
        }
    };
    match event.request_context.event_type {
        EventType::Connect => {
            println!("connecting {}", connection.id);
            DDB.with(|ddb| {
                println!(
                    "{:#?}",
                    ddb.put_item(PutItemInput {
                        table_name,
                        item: connection.into(),
                        ..PutItemInput::default()
                    })
                    .sync()
                );
            });
        }
        EventType::Disconnect => {
            println!("disconnecting {}", connection.id);
            DDB.with(|ddb| {
                println!(
                    "{:#?}",
                    ddb.delete_item(DeleteItemInput {
                        table_name,
                        key: connection.key(),
                        ..DeleteItemInput::default()
                    })
                    .sync()
                );
            });
        }
    }
    Ok(json!({
        "statusCode": 200
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deserialize_connect_event() {
        serde_json::from_str::<Event>(include_str!("../tests/data/connect.json"))
            .expect("failed to deserialize connect event");
    }
    #[test]
    fn deserialize_disconnect_event() {
        serde_json::from_str::<Event>(include_str!("../tests/data/disconnect.json"))
            .expect("failed to deserialize disconnect event");
    }
}
