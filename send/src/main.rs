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
use rusoto_core::error::RusotoError;

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

fn main() {
    env_logger::init();
    std::panic::set_hook(Box::new(|info| {
        println!("Custom panic hook {:#?}", info.payload().downcast_ref::<&str>());
    }));
    lambda!(handler)
}

fn handler(event: Event, _: Context) -> Result<Value, HandlerError> {
    let table_name = env::var("tableName")?;
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
            .for_each(move |item| {
                let _ = Connection::from_attrs(item).and_then(|connection| {
                    // https://docs.amazonaws.cn/en_us/apigateway/latest/developerguide/apigateway-how-to-call-websocket-api-connections.html
                    let client = ApiGatewayManagementApiClient::new(Region::Custom {
                        name: Region::UsEast1.name().into(),
                        endpoint: endpoint.clone(),
                    });
                    match client
                        .post_to_connection(PostToConnectionRequest {
                            connection_id: connection.id.clone(),
                            data: b"test".to_vec()
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
                                RusotoError::Service(PostToConnectionError::Gone(_)) => {
                                    // todo: delete the ddb connection id
                                    // this client is disconnected
                                }
                                _ => ()
                            }
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
