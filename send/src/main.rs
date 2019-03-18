use dynomite::{
    dynamodb::{DynamoDbClient, ScanInput},
    DynamoDbExt, FromAttributes, Item,
};
use futures::stream::Stream;
use lambda_runtime::{error::HandlerError, lambda, Context};
use rusoto_apigatewaymanagementapi::{
    ApiGatewayManagementApi, ApiGatewayManagementApiClient, PostToConnectionError,
    PostToConnectionRequest,
};
use rusoto_core::Region;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::env;
use tokio::runtime::Runtime;

thread_local!(
    static DDB: DynamoDbClient = DynamoDbClient::new(Region::default());
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
    body: String // parse this into json
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RequestContext {
    domain_name: String,
    stage: String,
}

fn main() {
    lambda!(handler)
}

fn handler(raw: Value, _: Context) -> Result<Value, HandlerError> {
    let event = match serde_json::from_value::<Event>(raw) {
        Ok(event) => event,
        Err(err) => {
            return Ok(json!({
                "statusCode": 500
            }))
        }
    };
    let table_name = match env::var("tableName") {
        Ok(table) => table,
        _ => {
            return Ok(json!({
                "statusCode" : 200
            }))
        }
    };
    let endpoint = format!(
        "https://{}/{}",
        event.request_context.domain_name, event.request_context.stage
    );
    let delivery = DDB.with(|ddb| {
        ddb.clone()
            .scan_pages(ScanInput {
                table_name,
                ..ScanInput::default()
            })
            .for_each(|item| {
                let _ = Connection::from_attrs(item).and_then(|connection| {
                    let client = ApiGatewayManagementApiClient::new(Region::Custom {
                        name: Region::UsEast1.name().into(),
                        endpoint: endpoint.clone(),
                    });
                    match client
                        .post_to_connection(PostToConnectionRequest {
                            connection_id: connection.id.clone(),
                            data: serde_json::to_vec(&json!({
                                "message": "got a message"
                            }))
                            .unwrap_or_default(),
                        })
                        .sync()
                    {
                        Ok(resp) => {
                            println!(
                                "post result to {} for connection {}: {:#?}",
                                endpoint.clone(),
                                connection.id.clone(),
                                resp
                            );
                        }
                        Err(err) => {
                            match err {
                                PostToConnectionError::Unknown(resp) => {
                                    println!(
                                        "post result to {} for connection {} {:?} {:#?}",
                                        endpoint.clone(),
                                        connection.id.clone(),
                                        resp.status,
                                        String::from_utf8_lossy(&resp.body)
                                    );
                                }
                                _ => println!("other error"),
                            };
                        }
                    }
                    Ok(())
                });
                Ok(())
            })
    });
    println!("{:#?}", RT.with(|rt| rt.borrow_mut().block_on(delivery)));
    Ok(json!({
        "statusCode": 200
    }))
}
