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

A typical websocket server requires an ability to a binary protocol over an upgraded
http protocol connection. By its nature it requires operational ability to maintain a
persistant connection with any connected clients. Secure websocket connects require additional handshake procedures.

### Serverless websocket "servers"

APIGateway replaces the need for you to write and operator traditional websocket servers. APIGateway manages exposing a tls (wss) websocket endpoint and persistent connections for you. Your application need only implement functions to be invoked to specific lifecycle events called "routes". A few special routes are `$connect` `$disconnect` and `$default` which represent a new client connecting, an existing client disconnecting, and an unmapped request route respectively. You can also route based on a request pattern. By default its expected clients send JSON with an "action" field which gets routed on by value. This example application routes on an action called "send".


Doug Tangren (softprops) 2019