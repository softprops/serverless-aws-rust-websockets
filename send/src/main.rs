use dynomite::AttributeError;
use dynomite::{
    dynamodb::{DeleteItemInput, DynamoDb, DynamoDbClient, ScanError, ScanInput},
    DynamoDbExt, FromAttributes, Item,
};
use futures::stream::Stream;
use lambda_runtime::{error::HandlerError, lambda, Context};
use rusoto_apigatewaymanagementapi::{
    ApiGatewayManagementApi, ApiGatewayManagementApiClient, PostToConnectionError,
    PostToConnectionRequest,
};
use rusoto_core::{Region, RusotoError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{cell::RefCell, env};
use tokio::runtime::Runtime;

thread_local!(
    static DDB: DynamoDbClient = DynamoDbClient::new(Default::default());
);

thread_local!(
    static RT: RefCell<Runtime> =
        RefCell::new(Runtime::new().expect("failed to initialize runtime"));
);

#[derive(Item)]
struct Connection {
    #[hash]
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
    Scan(ScanError),
    Deserialize(AttributeError),
}

fn main() {
    env_logger::init();
    lambda!(deliver)
}

fn endpoint(ctx: &RequestContext) -> String {
    format!("https://{}/{}", ctx.domain_name, ctx.stage)
}

fn deliver(event: Event, _: Context) -> Result<Value, HandlerError> {
    log::debug!("recv {}", event.body);
    let message = event
        .message()
        .and_then(|m| m.message)
        .unwrap_or_else(|| "pong".into());
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
            .for_each(move |item| {
                Connection::from_attrs(item)
                    .map_err(Error::Deserialize)
                    .and_then(|connection| {
                        // https://docs.amazonaws.cn/en_us/apigateway/latest/developerguide/apigateway-how-to-call-websocket-api-connections.html
                        if let Err(RusotoError::Service(PostToConnectionError::Gone(_))) = client
                            .clone()
                            .post_to_connection(PostToConnectionRequest {
                                connection_id: connection.id.clone(),
                                data: serde_json::to_vec(&json!({ "message": message }))
                                    .unwrap_or_default(),
                            })
                            .sync()
                        {
                            log::info!("hanging up on disconnected client {}", connection.id);
                            if let Err(err) = sweeper
                                .delete_item(DeleteItemInput {
                                    table_name: env::var("tableName")
                                        .expect("failed to resolve table"),
                                    key: connection.key(),
                                    ..DeleteItemInput::default()
                                })
                                .sync()
                            {
                                log::info!(
                                    "failed to delete connection {}: {}",
                                    connection.id,
                                    err
                                );
                            }
                        }
                        Ok(())
                    })
            })
    });

    if let Err(err) = RT.with(|rt| rt.borrow_mut().block_on(delivery)) {
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
