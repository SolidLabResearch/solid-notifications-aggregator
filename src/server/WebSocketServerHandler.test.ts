jest.mock('../utils/Util', () => ({ extract_ldp_inbox: jest.fn() }));

import { EventEmitter } from 'events';
import { extract_ldp_inbox } from '../utils/Util';
import { WebSocketServerHandler } from './WebSocketServerHandler';

type Connection = EventEmitter & { sendUTF: jest.Mock };

function connect(handler: WebSocketServerHandler, connection: Connection): void {
    const server = handler.websocket_server as EventEmitter;
    handler.handle_communication();
    server.emit('request', { origin: 'test', accept: () => connection });
}

async function flush(): Promise<void> { await new Promise(resolve => setImmediate(resolve)); await new Promise(resolve => setImmediate(resolve)); }

describe('WebSocketServerHandler subscription readiness', () => {
    beforeEach(() => jest.clearAllMocks());

    it('sends one ACK only after the upstream subscription resolves', async () => {
        const server = new EventEmitter(); const handler = new WebSocketServerHandler(server as any);
        const connection = Object.assign(new EventEmitter(), { sendUTF: jest.fn() }) as Connection;
        let resolveSubscription!: (value: boolean) => void;
        (extract_ldp_inbox as jest.Mock).mockResolvedValue('https://pod.example/inbox/');
        handler.subscribe_notification.subscribe_inbox = jest.fn(() => new Promise(resolve => { resolveSubscription = resolve; }));
        connect(handler, connection); connection.emit('message', { type: 'utf8', utf8Data: JSON.stringify({ subscribe: ['https://pod.example/stream/'] }) });
        await flush(); expect(connection.sendUTF).not.toHaveBeenCalled();
        resolveSubscription(true); await flush();
        expect(connection.sendUTF).toHaveBeenCalledTimes(1);
        expect(JSON.parse(connection.sendUTF.mock.calls[0][0])).toEqual({ type: 'subscription_ready', stream: 'https://pod.example/stream/' });
    });

    it('does not ACK a failed subscription', async () => {
        const server = new EventEmitter(); const handler = new WebSocketServerHandler(server as any);
        const connection = Object.assign(new EventEmitter(), { sendUTF: jest.fn() }) as Connection;
        (extract_ldp_inbox as jest.Mock).mockResolvedValue('inbox'); handler.subscribe_notification.subscribe_inbox = jest.fn().mockRejectedValue(new Error('upstream failed'));
        connect(handler, connection); connection.emit('message', { type: 'utf8', utf8Data: JSON.stringify({ subscribe: ['stream'] }) }); await flush();
        expect(connection.sendUTF).not.toHaveBeenCalled();
    });

    it('shares one upstream subscription while ACKing each client', async () => {
        const server = new EventEmitter(); const handler = new WebSocketServerHandler(server as any); const first = Object.assign(new EventEmitter(), { sendUTF: jest.fn() }) as Connection; const second = Object.assign(new EventEmitter(), { sendUTF: jest.fn() }) as Connection;
        (extract_ldp_inbox as jest.Mock).mockResolvedValue('inbox'); handler.subscribe_notification.subscribe_inbox = jest.fn().mockResolvedValue(true);
        connect(handler, first); server.emit('request', { origin: 'test', accept: () => second }); first.emit('message', { type: 'utf8', utf8Data: JSON.stringify({ subscribe: ['stream'] }) }); second.emit('message', { type: 'utf8', utf8Data: JSON.stringify({ subscribe: ['stream'] }) }); await flush();
        expect(handler.subscribe_notification.subscribe_inbox).toHaveBeenCalledTimes(1); expect(first.sendUTF).toHaveBeenCalledTimes(1); expect(second.sendUTF).toHaveBeenCalledTimes(1);
    });
});
