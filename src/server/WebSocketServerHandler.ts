import * as WebSocket from 'websocket';
import { SubscribeNotification } from '../service/SubscribeNotification';
import { extract_ldp_inbox } from '../utils/Util';

/**
 *
 */
export class WebSocketServerHandler {

    public websocket_server: any;
    public websocket_connections: Map<string, WebSocket[]>;
    public subscribe_notification: SubscribeNotification;
    /** One in-flight/completed upstream Solid subscription per exact stream URL. */
    private readonly stream_subscriptions: Map<string, Promise<void>>;

    /**
     * Creates an instance of WebSocketServerHandler.
     * @param {WebSocket.server} websocket_server - The WebSocket server.
     */
    constructor(websocket_server: WebSocket.server) {
        this.websocket_server = websocket_server;
        this.websocket_connections = new Map<string, WebSocket[]>();
        this.subscribe_notification = new SubscribeNotification();
        this.stream_subscriptions = new Map<string, Promise<void>>();
    }

    /**
     * Handles the communication for the WebSocket server.
     */
    public async handle_communication() {
        console.log(`Handling the communication for the WebSocket server.`);
        this.websocket_server.on('connect', (connection: any) => {
            console.log(`Connection received from the client with address: ${connection.remoteAddress}`);
        });

        this.websocket_server.on('request', (request: any) => {
            const connection = request.accept('solid-stream-notifications-aggregator', request.origin);
            connection.on('message', async(message: any) => {
                if (message.type === 'utf8') {
                    const message_utf8 = message.utf8Data;
                    const ws_message = JSON.parse(message_utf8);
                    if (Object.keys(ws_message).includes('subscribe')) {
                        console.log(`Received a subscribe message from the client.`);
                        const stream_to_subscribe = ws_message.subscribe;
                        for (const stream of stream_to_subscribe) {
                            console.log(`Subscribed to the stream: ${stream}`);
                            try {
                                await this.set_connections(stream, connection);
                                // This is deliberately after the successful Solid
                                // subscription and connection association, not after
                                // receipt of the client WebSocket message.
                                connection.sendUTF(JSON.stringify({ type: 'subscription_ready', stream }));
                            } catch (error) {
                                console.error(`Failed to establish subscription for ${stream}: ${(error as Error).message}`);
                            }
                        }
                    }
                    else if (Object.keys(ws_message).includes('event')) {
                        console.log(`Received a new event message from the client.`);
                        const connection = this.websocket_connections.get(ws_message.stream);
                        if (connection !== undefined) {
                            for (const [stream, connections] of this.websocket_connections) {
                                if (stream == ws_message.stream) {
                                    for (const connection of connections) {
                                        connection.send(JSON.stringify(ws_message));
                                    }
                                }
                            }
                        }
                    }
                    else if (Object.keys(ws_message).includes('container_location')) {
                        console.log(`Received a new inbox container location message from the client.`);
                        const inbox_container_location = ws_message.container_location;
                        this.subscribe_notification.subscribe_inbox(inbox_container_location);
                        console.log(`Subscribed to the inbox container location: ${inbox_container_location}`);
                    }
                    else {
                        console.log(`Received an unknown message from the client with the following content: ${message_utf8}`);
                        console.log(`The message is not recognized and supported by the Solid Stream Notifications Aggregator.`);
                    }
                }
            });
        });
    }

    /**
     * Sets the connections for the WebSocket server's Map.
     * @param {string} subscribed_stream - The subscribed stream.
     * @param {WebSocket} connection - The WebSocket connection.
     */
    public async set_connections(subscribed_stream: string, connection: WebSocket): Promise<void> {
        const connections = this.websocket_connections.get(subscribed_stream) || [];
        if (!connections.includes(connection)) connections.push(connection);
        this.websocket_connections.set(subscribed_stream, connections);

        let subscription = this.stream_subscriptions.get(subscribed_stream);
        if (!subscription) {
            subscription = (async () => {
                const stream_inbox = await extract_ldp_inbox(subscribed_stream) as string;
                if (!stream_inbox) throw new Error(`No inbox found for ${subscribed_stream}.`);
                const established = await this.subscribe_notification.subscribe_inbox(stream_inbox);
                if (established !== true) throw new Error(`Subscription was not established for ${subscribed_stream}.`);
            })();
            this.stream_subscriptions.set(subscribed_stream, subscription);
        }
        try {
            await subscription;
        } catch (error) {
            const current = this.websocket_connections.get(subscribed_stream) || [];
            this.websocket_connections.set(subscribed_stream, current.filter(item => item !== connection));
            this.stream_subscriptions.delete(subscribed_stream);
            throw error;
        }
    }
}
