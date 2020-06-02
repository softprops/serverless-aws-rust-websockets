use dynomite::{
    dynamodb::{DeleteItemInput, DynamoDb, DynamoDbClient, PutItemInput},
    Item,
};
use lambda::handler_fn;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

thread_local!(
    static DDB: DynamoDbClient = DynamoDbClient::new(Default::default());
);

#[derive(Item, Clone)]
struct Connection {
    #[dynomite(partition_key)]
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    lambda::run(handler_fn(connector)).await?;
    Ok(())
}

async fn connector(
    event: Event
) -> Result<Value, Error> {
    let table_name = env::var("tableName")?;
    let connection = Connection {
        id: event.request_context.connection_id,
    };
    match event.request_context.event_type {
        EventType::Connect => {
            log::info!("connecting {}", connection.id);
            DDB.with(|ddb| {
                let ddb = ddb.clone();
                async move {
                    if let Err(err) = ddb
                        .put_item(PutItemInput {
                            table_name,
                            item: connection.clone().into(),
                            ..PutItemInput::default()
                        })
                        .await
                    {
                        log::error!("failed to perform connection operation: {:?}", err);
                    }
                }
            })
            .await;
        }
        EventType::Disconnect => {
            log::info!("disconnecting {}", connection.id);
            DDB.with(|ddb| {
                let ddb = ddb.clone();
                async move {
                    if let Err(err) = ddb
                        .delete_item(DeleteItemInput {
                            table_name,
                            key: connection.key(),
                            ..DeleteItemInput::default()
                        })
                        .await
                    {
                        log::error!("failed to perform disconnection operation: {:?}", err);
                    }
                }
            })
            .await;
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
