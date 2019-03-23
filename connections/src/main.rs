use dynomite::{
    dynamodb::{
        DeleteItemError, DeleteItemInput, DynamoDb, DynamoDbClient, PutItemError, PutItemInput,
    },
    Item,
};
use futures::{future::Either, Future};
use lambda_runtime::{error::HandlerError, lambda, Context};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::env;
use tokio::runtime::Runtime;

thread_local!(
    static DDB: DynamoDbClient = DynamoDbClient::new(Default::default());
);

thread_local!(
    static RT: RefCell<Runtime> =
        RefCell::new(Runtime::new().expect("failed to initialize runtime"));
);

#[derive(Item, Clone)]
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
    env_logger::init();
    lambda!(connector)
}

#[derive(Debug)]
enum Error {
    Connect(PutItemError),
    Disconnect(DeleteItemError),
}

fn connector(event: Event, _: Context) -> Result<Value, HandlerError> {
    let table_name = env::var("tableName")?;
    let connection = Connection {
        id: event.request_context.connection_id,
    };
    let result = match event.request_context.event_type {
        EventType::Connect => {
            log::info!("connecting {}", connection.id);
            DDB.with(|ddb| {
                Either::A(
                    ddb.put_item(PutItemInput {
                        table_name,
                        item: connection.clone().into(),
                        ..PutItemInput::default()
                    })
                    .map(drop)
                    .map_err(Error::Connect),
                )
            })
        }
        EventType::Disconnect => {
            log::info!("disconnecting {}", connection.id);
            DDB.with(|ddb| {
                Either::B(
                    ddb.delete_item(DeleteItemInput {
                        table_name,
                        key: connection.key(),
                        ..DeleteItemInput::default()
                    })
                    .map(drop)
                    .map_err(Error::Disconnect),
                )
            })
        }
    };

    if let Err(err) = RT.with(|rt| rt.borrow_mut().block_on(result)) {
        log::error!("failed to perform connection operation: {:?}", err);
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
