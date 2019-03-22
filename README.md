# serverless aws websockets

exploration into Rust and [serverless websockets](https://serverless.com/framework/docs/providers/aws/events/websocket/)


## deploy

```sh
$ npm i && npx serverless deploy
```

You can use the `wscat` command line utility to connect and communicate with your
serverless application.

```sh
$ npx wscat -c wss://{YOUR-API-ID}.execute-api.{YOUR-REGION}.amazonaws.com/dev
```

## how it works

### Traditional servers

A typical websocket server requires an ability to speak a binary protocol over an upgraded
http protocol connection. By its nature it requires operational ability to maintain a
persistant connection with any connected clients. Secure websocket connects require additional handshake procedures.

### Serverless websocket "servers"

APIGateway replaces the need for traditional websocket servers. It manages exposing tls (by default!) endpoint and persistent connections for you. Your application reacts to specific events called "routes". A few special routes are `$connect` `$disconnect` and `$default`. You can also route based on a request pattern. By default its expected clients send JSON with an "action" field which gets routed on by value. This example application routes on an action called "send".




Doug Tangren (softprops) 2019
