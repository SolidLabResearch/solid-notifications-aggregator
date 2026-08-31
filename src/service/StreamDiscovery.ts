import axios from 'axios';
import { Parser, Store } from 'n3';

const PUBLIC_TYPE_INDEX = 'http://www.w3.org/ns/solid/terms#publicTypeIndex';
const RELATES_TO_PROPERTY = 'https://saref.etsi.org/core/relatesToProperty';
const TREE_VIEW = 'https://w3id.org/tree#view';

export interface DiscoveryTimingObserver {
    publicTypeIndexStart?: () => void;
    publicTypeIndexEnd?: (url: string) => void;
    relevantStreamsEnd?: (streams: string[]) => void;
}

/**
 * Resolves LDES streams advertised by a Solid Pod's Public Type Index.
 *
 * The request order deliberately mirrors Heimdall: first retrieve the profile
 * and Type Index to check whether one requested metric exists, then retrieve
 * them again before collecting tree:view values.  There is intentionally no
 * cache or request parallelism here so discovery can later be timed separately.
 */
export class StreamDiscovery {
    /** Finds the Public Type Index URL advertised by the pod profile. */
    public async findPublicTypeIndex(podUrl: string, observer?: DiscoveryTimingObserver): Promise<string> {
        const profileUrl = this.profileUrl(podUrl);
        observer?.publicTypeIndexStart?.();
        const profile = await this.getStore(profileUrl, 'profile/card');
        const typeIndex = profile.getQuads(null, PUBLIC_TYPE_INDEX, null, null)[0];
        if (!typeIndex) {
            throw new Error(`Public Type Index is missing from ${profileUrl}.`);
        }
        observer?.publicTypeIndexEnd?.(typeIndex.object.value);
        return typeIndex.object.value;
    }

    /**
     * Finds streams using the same two-pass procedure currently used by Heimdall.
     *
     * As in Heimdall, once any interest metric is found this returns every
     * tree:view in the Type Index rather than joining it to that metric's
     * registration subject.
     */
    public async findRelevantStreams(podUrl: string, interestMetrics: string[], observer?: DiscoveryTimingObserver): Promise<string[]> {
        if (!Array.isArray(interestMetrics) || interestMetrics.length === 0 || interestMetrics.some(metric => typeof metric !== 'string' || metric.length === 0)) {
            throw new Error('At least one non-empty metric URI is required for stream discovery.');
        }

        // First pass: equivalent to Heimdall's if_exists_relevant_streams().
        const firstTypeIndex = await this.findPublicTypeIndex(podUrl, observer);
        const firstStore = await this.getStore(firstTypeIndex, 'Public Type Index');
        const metricFound = firstStore.getQuads(null, RELATES_TO_PROPERTY, null, null)
            .some(quad => interestMetrics.includes(quad.object.value));
        if (!metricFound) {
            throw new Error(`Requested metric is not present in Public Type Index ${firstTypeIndex}.`);
        }

        // Second pass: equivalent to Heimdall's find_relevant_streams().
        const secondTypeIndex = await this.findPublicTypeIndex(podUrl, observer);
        const secondStore = await this.getStore(secondTypeIndex, 'Public Type Index');
        const streams = secondStore.getQuads(null, TREE_VIEW, null, null).map(quad => quad.object.value);
        if (streams.length === 0) {
            throw new Error(`No tree:view can be found in Public Type Index ${secondTypeIndex}.`);
        }
        observer?.relevantStreamsEnd?.(streams);
        return streams;
    }

    private profileUrl(podUrl: string): string {
        if (typeof podUrl !== 'string' || podUrl.trim().length === 0) {
            throw new Error('A non-empty Solid Pod URL is required for stream discovery.');
        }
        try {
            return new URL('profile/card', `${podUrl.trim().replace(/\/+$/, '')}/`).toString();
        } catch (error) {
            throw new Error(`Invalid Solid Pod URL ${podUrl}: ${(error as Error).message}`);
        }
    }

    private async getStore(url: string, resourceName: string): Promise<Store> {
        let response: { data: string };
        try {
            response = await axios.get(url);
        } catch (error) {
            throw new Error(`Could not retrieve ${resourceName} from ${url}: ${(error as Error).message}`);
        }
        try {
            return new Store(new Parser().parse(response.data));
        } catch (error) {
            throw new Error(`Malformed RDF in ${resourceName} ${url}: ${(error as Error).message}`);
        }
    }
}
