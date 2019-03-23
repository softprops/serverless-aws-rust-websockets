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
use rusoto_core::Region;
use rusoto_core::RusotoError;
use serde::Deserialize;
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

#[derive(Item)]
struct Connection {
    #[hash]
    id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Event {
    request_context: RequestContext,
    body: String, // parse this into json
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

fn deliver(event: Event, _: Context) -> Result<Value, HandlerError> {
    log::debug!("recv {}", event.body);
    let table_name = env::var("tableName")?;
    let endpoint = format!(
        "https://{}/{}",
        event.request_context.domain_name, event.request_context.stage
    );
    let client = ApiGatewayManagementApiClient::new(Region::Custom {
        name: Region::UsEast1.name().into(),
        endpoint,
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
                                data: serde_json::to_vec(&json!({
                                    "message": "pong"
                                }))
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
