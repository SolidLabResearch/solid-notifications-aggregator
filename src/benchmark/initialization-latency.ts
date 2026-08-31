import { performance } from 'node:perf_hooks';
import { StreamDiscovery, DiscoveryTimingObserver } from '../service/StreamDiscovery';
import { extract_ldp_inbox, NotificationTimingObserver } from '../utils/Util';
import { SubscribeNotification } from '../service/SubscribeNotification';

export interface InitializationTimingResult { system: 'notifications_aggregator'; resolved_stream: string; public_type_index_discovery_ms: number; relevant_stream_discovery_ms: number; stream_discovery_total_ms: number; inbox_discovery_ms: number; subscription_server_discovery_ms: number; webhook_subscription_creation_ms: number; notification_subscription_total_ms: number; discovery_and_subscription_total_ms: number; subscription_channel?: string; success: boolean; error?: string; }
const elapsed = (a: number, b: number): number => Math.max(0, b - a);
const required = (name: string): string => { const value = process.env[name]; if (!value) throw new Error(`Missing required benchmark configuration: ${name}`); return value; };

export async function runInitializationBenchmarkOnce(): Promise<InitializationTimingResult> {
    const pod = required('BENCHMARK_POD_URL'); const metric = required('BENCHMARK_METRIC_URI');
    const t: any = {}; let count = 0; let dStart = performance.now();
    const dObs: DiscoveryTimingObserver = { publicTypeIndexStart: () => { if (count === 0) dStart = performance.now(); }, publicTypeIndexEnd: () => { if (count++ === 0) { t.d1End = performance.now(); t.d2Start = t.d1End; } }, relevantStreamsEnd: () => { t.d2End = performance.now(); } };
    const nObs: NotificationTimingObserver = { inboxStart: () => { t.s1Start = performance.now(); }, inboxEnd: () => { t.s1End = performance.now(); }, subscriptionServerStart: () => { t.s2Start = performance.now(); }, subscriptionServerEnd: () => { t.s2End = performance.now(); }, subscriptionCreationStart: () => { t.s3Start = performance.now(); }, subscriptionResponse: (ok, channel) => { t.s3End = performance.now(); t.ok = ok; t.channel = channel; } };
    try {
        const totalStart = performance.now(); const streams = await new StreamDiscovery().findRelevantStreams(pod, [metric], dObs); const stream = streams[0]; if (!stream) throw new Error('No relevant LDES stream found.');
        const dEnd = performance.now(); const inbox = await extract_ldp_inbox(stream, nObs); if (!inbox) throw new Error('No LDP inbox found.');
        const ok = await new SubscribeNotification().subscribe_inbox(inbox, nObs); if (ok !== true || !t.ok) throw new Error('Webhook subscription did not return a successful response.');
        const end = performance.now(); return { system: 'notifications_aggregator', resolved_stream: stream, public_type_index_discovery_ms: elapsed(dStart, t.d1End), relevant_stream_discovery_ms: elapsed(t.d2Start, t.d2End), stream_discovery_total_ms: elapsed(totalStart, dEnd), inbox_discovery_ms: elapsed(t.s1Start, t.s1End), subscription_server_discovery_ms: elapsed(t.s2Start, t.s2End), webhook_subscription_creation_ms: elapsed(t.s3Start, t.s3End), notification_subscription_total_ms: elapsed(t.s1Start, t.s3End), discovery_and_subscription_total_ms: elapsed(totalStart, end), subscription_channel: t.channel, success: true };
    } catch (error) { return { system: 'notifications_aggregator', resolved_stream: '', public_type_index_discovery_ms: 0, relevant_stream_discovery_ms: 0, stream_discovery_total_ms: 0, inbox_discovery_ms: 0, subscription_server_discovery_ms: 0, webhook_subscription_creation_ms: 0, notification_subscription_total_ms: 0, discovery_and_subscription_total_ms: 0, success: false, error: (error as Error).message }; }
}
if (require.main === module) runInitializationBenchmarkOnce().then(result => console.log(`BENCHMARK_RESULT=${JSON.stringify(result)}`));
