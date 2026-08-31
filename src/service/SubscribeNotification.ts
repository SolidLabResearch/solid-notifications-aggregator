import { extract_ldp_inbox, extract_subscription_server, NotificationTimingObserver } from "../utils/Util";
import * as WebSocket from 'websocket';
import * as AGGREGATOR_SETUP from '../config/notif_aggregator_setup.json';
import axios from 'axios';

/**
 * This class is used to subscribe to the notification server for real-time notifications.
 * @class SubscribeNotification
 */
export class SubscribeNotification {
    /**
     * Creates an instance of SubscribeNotification.
     * @memberof SubscribeNotification
     */
    constructor() {
    }

    /**
     * Subscribes to the notification server for each LDES stream in the constructor, using the inbox and subscription server.
     * @param {string} ldes_stream - The LDES stream to subscribe to.
     * @returns {(Promise<boolean | undefined>)} - Returns a promise with a boolean or undefined. If the subscription is successful, it returns true. If the subscription fails, it throws an error.
     * @memberof SubscribeNotification
     */
    public async subscribe_stream(ldes_stream: string): Promise<boolean | undefined> {
        const inbox = await extract_ldp_inbox(ldes_stream) as string;
        console.log(`Inbox location: ${inbox}`);
        const subscription_server = await extract_subscription_server(inbox);
        if (subscription_server === undefined) {
            throw new Error("Subscription server is undefined.");
        } else {
            const body = {
                "@context": ["https://www.w3.org/ns/solid/notification/v1"],
                "type": "http://www.w3.org/ns/solid/notifications#WebhookChannel2023",
                "topic": ldes_stream,
                "sendTo": `${AGGREGATOR_SETUP.notif_aggregator_http_server_url}`
            }

            const response_subscribe_ldes_stream = await axios.post(subscription_server.location, body, {
                headers: {
                    'Content-Type': 'application/ld+json'
                }
            });
            if (response_subscribe_ldes_stream.status === 200) {
                return true;
            }
            else {
                throw new Error("The subscription to the notification server failed.");
            }
        }
    }

    /**
     * Subscribes to the notification server for a specific inbox location.
     * @param {string} inbox_location - The inbox location to subscribe to.
     * @returns {(Promise<boolean | undefined>)} - Returns a promise with a boolean or undefined. If the subscription is successful, it returns true. If the subscription fails, it throws an error.
     */
    public async subscribe_inbox(inbox_location:string, observer?: NotificationTimingObserver): Promise<boolean | undefined> {
        const subscription_server = await extract_subscription_server(inbox_location, observer);
        if (subscription_server === undefined) {
            throw new Error("Subscription server is undefined.");
        } else {
            const body = {
                "@context": ["https://www.w3.org/ns/solid/notification/v1"],
                "type": "http://www.w3.org/ns/solid/notifications#WebhookChannel2023",
                "topic": inbox_location,
                "sendTo": `${AGGREGATOR_SETUP.notif_aggregator_http_server_url}`
            };


            observer?.subscriptionCreationStart?.();
            const response_subscribe_ldes_stream = await axios.post(subscription_server.location, body, {
                headers: {
                    'Content-Type': 'application/ld+json'
                }
            });
            observer?.subscriptionResponse?.(response_subscribe_ldes_stream.status === 200, response_subscribe_ldes_stream.data?.id);
            if (response_subscribe_ldes_stream.status === 200) {
                console.log(`Subscribed to the inbox container location: ${inbox_location}`);
                return true;
            }
            else {
                throw new Error("The subscription to the notification server failed.");
            }
        }
    }

    /**
     * Checks if the LDES stream is already subscribed to the notification server.
     * @param {string} ldes_stream - The LDES stream to check if it is already subscribed.
     * @param {Map<string, WebSocket[]>} websocket_connections - The WebSocket connections.
     * @returns {Promise<boolean>} - Returns a promise with a boolean. If the LDES stream is already subscribed, it returns true. If the LDES stream is not subscribed, it returns false.
     * @memberof SubscribeNotification
     */
    public async check_if_aleady_subscribed(ldes_stream: string, websocket_connections: Map<string, WebSocket[]>): Promise<boolean> {
        if (websocket_connections.has(ldes_stream)) {
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Sets the connections for the WebSocket server's Map.
     * @param {string} ldes_stream - The LDES stream to subscribe to.
     * @param {Map<string, WebSocket>} websocket_connections - The WebSocket connections.
     * @returns {(Promise<WebSocket | undefined>)} - Returns a promise with a WebSocket or undefined. If the connection is set, it returns the WebSocket connection. If the connection is not set, it returns undefined.
     * @memberof SubscribeNotification
     */
    public async get_connection(ldes_stream: string, websocket_connections: Map<string, WebSocket>): Promise<WebSocket | undefined> {
        return websocket_connections.get(ldes_stream);
    }
}
