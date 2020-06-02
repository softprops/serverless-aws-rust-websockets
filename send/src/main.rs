use dynomite::{
    dynamodb::{DeleteItemInput, DynamoDb, DynamoDbClient, ScanError, ScanInput},
    AttributeError, DynamoDbExt, FromAttributes, Item,
};
use futures::TryStreamExt;
use lambda::handler_fn;
use rusoto_apigatewaymanagementapi::{
    ApiGatewayManagementApi, ApiGatewayManagementApiClient, PostToConnectionError,
    PostToConnectionRequest,
};
use rusoto_core::{Region, RusotoError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::env;

thread_local!(
    static DDB: DynamoDbClient = DynamoDbClient::new(Default::default());
);

#[derive(Item)]
struct Connection {
    #[dynomite(partition_key)]
    id: String,
}

/// the structure of the client payload (action aside)
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Message {
    message: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Event {
    request_context: RequestContext,
    body: String, // parse this into json
}

impl Event {
    fn message(&self) -> Option<Message> {
        serde_json::from_str::<Message>(&self.body).ok()
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RequestContext {
    domain_name: String,
    stage: String,
}

#[derive(Debug)]
enum Error {
    Scan(RusotoError<ScanError>),
    Deserialize(AttributeError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
    env_logger::init();
    lambda::run(handler_fn(deliver)).await?;
    Ok(())
}

fn endpoint(ctx: &RequestContext) -> String {
    format!("https://{}/{}", ctx.domain_name, ctx.stage)
}

async fn deliver(
    event: Event
) -> Result<Value, Box<dyn std::error::Error + Sync + Send + 'static>> {
    log::debug!("recv {}", event.body);
    let message = event
        .message()
        .and_then(|m| m.message)
        .unwrap_or_else(|| "🏓 pong".into());
    let table_name = env::var("tableName")?;
    let client = ApiGatewayManagementApiClient::new(Region::Custom {
        name: Region::UsEast1.name().into(),
        endpoint: endpoint(&event.request_context),
    });
    let delivery = DDB.with(|ddb| {
        let sweeper = ddb.clone();
        ddb.clone()
            .scan_pages(ScanInput {
                table_name,
                ..ScanInput::default()
            })
            .map_err(Error::Scan)
            .try_for_each(move |item| {
                let client = client.clone();
                let sweeper = sweeper.clone();
                let message = message.clone();
                async move {
                    match Connection::from_attrs(item) {
                        Err(err) => return Err(Error::Deserialize(err)),
                        Ok(connection) => {
                            // https://docs.amazonaws.cn/en_us/apigateway/latest/developerguide/apigateway-how-to-call-websocket-api-connections.html
                            if let Err(RusotoError::Service(PostToConnectionError::Gone(_))) =
                                client
                                    .post_to_connection(PostToConnectionRequest {
                                        connection_id: connection.id.clone(),
                                        data: serde_json::to_vec(&json!({ "message": message }))
                                            .unwrap_or_default()
                                            .into(),
                                    })
                                    .await
                            {
                                log::info!("hanging up on disconnected client {}", connection.id);
                                if let Err(err) = sweeper
                                    .delete_item(DeleteItemInput {
                                        table_name: env::var("tableName")
                                            .expect("failed to resolve table"),
                                        key: connection.key(),
                                        ..DeleteItemInput::default()
                                    })
                                    .await
                                {
                                    log::info!(
                                        "failed to delete connection {}: {}",
                                        connection.id,
                                        err
                                    );
                                }
                            }
                        }
                    }

                    Ok(())
                }
            })
    });

    if let Err(err) = delivery.await {
        log::error!("failed to deliver message: {:?}", err);
    }

    Ok(json!({
        "statusCode": 200
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_send_event_with_message() {
        let event =
            serde_json::from_str::<Event>(include_str!("../tests/data/send-something.json"))
                .expect("failed to deserialize send event");
        assert_eq!(
            event.message().and_then(|m| m.message),
            Some("howdy".into())
        )
    }

    #[test]
    fn deserialize_send_event_without_message() {
        let event = serde_json::from_str::<Event>(include_str!("../tests/data/send-nothing.json"))
            .expect("failed to deserialize send event");
        assert_eq!(event.message(), None)
    }

    #[test]
    fn formats_endpoint() {
        assert_eq!(
            endpoint(&RequestContext {
                domain_name: "xxx.execute-api.us-east-1.amazonaws.com".into(),
                stage: "dev".into()
            }),
            "https://xxx.execute-api.us-east-1.amazonaws.com/dev"
        )
    }
}
