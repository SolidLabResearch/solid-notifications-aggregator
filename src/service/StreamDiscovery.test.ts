import axios from 'axios';
import { StreamDiscovery } from './StreamDiscovery';

jest.mock('axios');

const pod = 'http://localhost:3000/pod/';
const profile = '<#me> <http://www.w3.org/ns/solid/terms#publicTypeIndex> <http://localhost:3000/pod/publicTypeIndex> .';
const metricX = 'https://example.test/metric/x';
const metricY = 'https://example.test/metric/y';
const typeIndex = `
    <#x> <https://saref.etsi.org/core/relatesToProperty> <${metricX}> ;
         <https://w3id.org/tree#view> <http://localhost:3000/pod/acc-x/> .
    <#y> <https://saref.etsi.org/core/relatesToProperty> <${metricY}> ;
         <https://w3id.org/tree#view> <http://localhost:3000/pod/acc-y/> .
`;

describe('StreamDiscovery', () => {
    let discovery: StreamDiscovery;

    beforeEach(() => {
        jest.resetAllMocks();
        discovery = new StreamDiscovery();
    });

    it('finds the public Type Index from profile/card with either pod URL form', async () => {
        (axios.get as jest.Mock).mockResolvedValue({ data: profile });

        await expect(discovery.findPublicTypeIndex('http://localhost:3000/pod')).resolves.toBe('http://localhost:3000/pod/publicTypeIndex');
        await expect(discovery.findPublicTypeIndex(pod)).resolves.toBe('http://localhost:3000/pod/publicTypeIndex');
        expect(axios.get).toHaveBeenNthCalledWith(1, 'http://localhost:3000/pod/profile/card');
        expect(axios.get).toHaveBeenNthCalledWith(2, 'http://localhost:3000/pod/profile/card');
    });

    it('finds streams for matching metrics using Heimdall\'s sequential two-pass request pattern', async () => {
        (axios.get as jest.Mock)
            .mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: typeIndex })
            .mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: typeIndex });

        await expect(discovery.findRelevantStreams(pod, [metricX])).resolves.toEqual([
            'http://localhost:3000/pod/acc-x/', 'http://localhost:3000/pod/acc-y/'
        ]);
        expect((axios.get as jest.Mock).mock.calls.map((call: unknown[]) => call[0])).toEqual([
            'http://localhost:3000/pod/profile/card', 'http://localhost:3000/pod/publicTypeIndex',
            'http://localhost:3000/pod/profile/card', 'http://localhost:3000/pod/publicTypeIndex'
        ]);
    });

    it('returns an explicit error when no requested metric is present', async () => {
        (axios.get as jest.Mock).mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: typeIndex });
        await expect(discovery.findRelevantStreams(pod, ['https://example.test/metric/unknown']))
            .rejects.toThrow('Requested metric is not present');
    });

    it('supports multiple requested metrics and streams', async () => {
        (axios.get as jest.Mock)
            .mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: typeIndex })
            .mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: typeIndex });
        await expect(discovery.findRelevantStreams(pod, [metricX, metricY])).resolves.toEqual([
            'http://localhost:3000/pod/acc-x/', 'http://localhost:3000/pod/acc-y/'
        ]);
    });

    it.each([
        ['cannot retrieve profile/card', () => (axios.get as jest.Mock).mockRejectedValue(new Error('offline')), 'Could not retrieve profile/card'],
        ['has no public Type Index', () => (axios.get as jest.Mock).mockResolvedValue({ data: '<#me> <https://example.test/p> <https://example.test/o> .' }), 'Public Type Index is missing'],
        ['cannot retrieve the Type Index', () => (axios.get as jest.Mock).mockResolvedValueOnce({ data: profile }).mockRejectedValueOnce(new Error('404')), 'Could not retrieve Public Type Index'],
        ['has malformed RDF', () => (axios.get as jest.Mock).mockResolvedValue({ data: '<unterminated' }), 'Malformed RDF'],
    ])('reports an explicit error when it %s', async(_case, arrange, expected) => {
        arrange();
        await expect(discovery.findRelevantStreams(pod, [metricX])).rejects.toThrow(expected);
    });

    it('reports an explicit error when no tree:view is present', async () => {
        const noView = `<#x> <https://saref.etsi.org/core/relatesToProperty> <${metricX}> .`;
        (axios.get as jest.Mock)
            .mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: noView })
            .mockResolvedValueOnce({ data: profile }).mockResolvedValueOnce({ data: noView });
        await expect(discovery.findRelevantStreams(pod, [metricX])).rejects.toThrow('No tree:view can be found');
    });
});
