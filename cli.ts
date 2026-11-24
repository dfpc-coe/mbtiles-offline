import { MBTilesOffline, validateUrl } from './index.js';
import Progress from 'ts-progress';
import { input, number } from '@inquirer/prompts';
import minimist from 'minimist';

const args = minimist(process.argv.slice(2));

function arg(name: string): string | undefined {
    const value: unknown = args[name];
    if (typeof value === 'string' && value !== '') return value;
    if (typeof value === 'number') return String(value);
    return undefined;
}

function urlError(url: string): string | undefined {
    try {
        validateUrl(url);
        return undefined;
    } catch (err) {
        return err instanceof Error ? err.message : String(err);
    }
}

function parseBounds(value: string): number[] {
    return value.split(',').map((p) => (p.trim() === '' ? Number.NaN : Number(p)));
}

function isBounds(parts: number[]): parts is [number, number, number, number] {
    return parts.length === 4 && !parts.some((p) => Number.isNaN(p));
}

function fail(message: string): never {
    console.error(`Error: ${message}`);
    process.exit(1);
}

const name = arg('name') ?? await input({
    message: 'Name:',
    default: 'External Imagery'
});

const description = arg('description') ?? await input({
    message: 'Description:',
    default: 'Downloaded with mbtiles-offline'
});

const version = arg('version') ?? await input({
    message: 'Version:',
    default: '1.0.0'
});

let url = arg('url');
if (url === undefined) {
    url = await input({
        message: 'Tile URL Template:',
        validate: (value: string) => urlError(value) ?? true
    });
} else {
    const error = urlError(url);
    if (error !== undefined) fail(error);
}

const output = arg('output') ?? await input({
    message: 'Output file:',
    default: 'tiles.mbtiles'
});

const minzoom = Number(arg('minzoom') ?? await number({
    message: 'Min Zoom:',
    default: 0
}));

const maxzoom = Number(arg('maxzoom') ?? await number({
    message: 'Max Zoom:',
    default: 14
}));

const bounds = parseBounds(arg('bounds') ?? await input({
    message: 'Bounds (minx, miny, maxx, maxy):',
    validate: (value: string) => isBounds(parseBounds(value)) || 'Bounds must be 4 comma-separated numbers'
}));

const concurrency = Number(arg('concurrency') ?? await number({
    message: 'Concurrency:',
    default: 100
}));

if (!isBounds(bounds)) {
    fail('Bounds must be an array of 4 numbers [minx, miny, maxx, maxy]');
}

const download = new MBTilesOffline({
    name,
    description,
    version,
    bounds,
    url,
    minzoom,
    maxzoom,
    output,
    concurrency,
    clip: args.clip !== false
});

let progressBar: ReturnType<typeof Progress.create> | undefined;
let currentTotal = 0;
download.on('total', (total: number) => {
    currentTotal = total;
    progressBar = Progress.create({
        total,
        pattern: 'Progress: {bar} | Remaining: {remaining} | {percent} | {current}/{total}',
        updateFrequency: 1
    });
}).on('progress', (progress: number) => {
    progressBar?.update();

    if (progress === currentTotal) {
        progressBar?.done();
        console.error('done');
    }
}).on('error', (err: unknown) => {
    console.error('Error:', err);
});

await download.start();

console.log('done');
