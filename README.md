## Solid Notifications Aggregator

The Solid Notifications Aggregator is a service which works on top of one or more Solid Pods to provide an immediate aggregator for the clients to consume the latest streaming data stored in the Solid Pods via the Solid Notifications Protocol's [WebHookChannel2023](https://solid.github.io/notifications/webhook-channel-2023).  


## Architecture

![Solid Notifications Aggregator Architecture](./architecture.png)


## Installation

To install the server, you can run the following command:
```bash
npm install
```

Make sure you have a Solid Server running which supports the WebHookChannel2023 for receiving notifications. We recommend using the [Community Solid Server](https://github.com/CommunitySolidServer/CommunitySolidServer) with the WebHook configuration which can be created using the [CSS Config Generator](https://communitysolidserver.github.io/configuration-generator/v7/).

## Usage

To run the server, you can run the server using the following command:
```bash
npm run start
```
The interface for the service is a Websocket server with which you can interact. The server accepts a subscribe message as shown below:

```json
{
    "subscribe": ["http://localhost:3000/aggregation_pod/aggregation/"]
}
```
which can be sent to the server to subscribe to the latest events from the particular LDES stream. The client can pass one or multiple LDES Streams into the subscribe message array to receive the events from the server.
The server will then send the events to the client as and when they are received. A sample script for the client is shown below:
```ts
import { WebSocket } from "ws";

async function main() {
    const websocket = new WebSocket('ws://localhost:8085//', 'solid-stream-notifications-aggregator', {
        perMessageDeflate: false
    });
    websocket.once('open', () => {
        console.log('Connection to the WebSocket server was successful.');
        let message_object = {
            subscribe: ['http://localhost:3000/aggregation_pod/aggregation/']
        };
        websocket.send(JSON.stringify(message_object));
    });

    websocket.on('message', (data) => {
        console.log(data.toString());
    });
}

main()

```
The client will receive message such as,
```json
{
    "stream": "stream_url",
    "published_time": "time_of_notification",
    "event": "the_notification_event"
}
```

## License
This code is copyrighted by [Ghent University - imec](https://www.ugent.be/ea/idlab/en) and released under the [MIT License](./LICENSE). 

## Contact

For any questions, please contact [Kush](mailto:kushbisen@proton.me) or create an issue in the repository. 