import { MBTilesOffline } from './index.js';
import Progress from 'ts-progress';
import { input, number } from '@inquirer/prompts';
import minimist from 'minimist';

const args = minimist(process.argv.slice(2));
const config: any = { ...args };

if (!config.name) {
    config.name = await input({
        message: 'Name:',
        default: 'External Imagery'
    });
}

if (!config.description) {
    config.description = await input({
        message: 'Description:',
        default: 'Downloaded with mbtiles-offline'
    });
}

if (!config.version) {
    config.version = await input({
        message: 'Version:',
        default: '1.0.0'
    });
}

if (!config.url) {
    config.url = await input({
        message: 'Tile URL Template:',
        validate: (input: string) => input.length > 0 ? true : 'URL is required'
    });
}

if (!config.output) {
    config.output = await input({
        message: 'Output file:',
        default: 'tiles.mbtiles'
    });
}

if (!config.minzoom) {
    config.minzoom = await number({
        message: 'Min Zoom:',
        default: 0
    });
}

if (!config.maxzoom) {
    config.maxzoom = await number({
        message: 'Max Zoom:',
        default: 14
    });
}

if (!config.bounds) {
    config.bounds = await input({
        message: 'Bounds (minx, miny, maxx, maxy):',
        validate: (input: string) => {
            const parts = input.split(',').map((p) => parseFloat(p.trim()));
            if (parts.length !== 4 || parts.some(isNaN)) {
                return 'Bounds must be 4 comma-separated numbers';
            }
            return true;
        }
    });
}

if (!config.concurrency) {
    config.concurrency = await number({
        message: 'Concurrency:',
        default: 100
    });
}

// Parse bounds if it's a string (from CLI or Inquirer string input)
if (typeof config.bounds === 'string') {
    config.bounds = config.bounds.split(',').map((p: string) => parseFloat(p.trim()));
}

if (!Array.isArray(config.bounds) || config.bounds.length !== 4 || config.bounds.some(isNaN)) {
    console.error('Error: Bounds must be an array of 4 numbers [minx, miny, maxx, maxy]');
    process.exit(1);
}

const download = new MBTilesOffline({
    name: config.name,
    description: config.description,
    version: config.version,
    bounds: config.bounds as [number, number, number, number],
    url: config.url,
    minzoom: Number(config.minzoom),
    maxzoom: Number(config.maxzoom),
    output: config.output,
    concurrency: Number(config.concurrency),
});

let progressBar: any;
let currentTotal: number;
let currentProgress: number;
download.on('total', (total) => {
    currentTotal = total;
    progressBar = Progress.create({
        total,
        pattern: 'Progress: {bar} | Remaining: {remaining} | {percent} | {current}/{total}',
        updateFrequency: 1,
    });
}).on('progress', (progress) => {
    currentProgress = progress;
    progressBar.update();

    if (currentProgress === currentTotal) {
        progressBar.done();
        console.error('done');
    }
}).on('error', (err) => {
    console.error('Error:', err);
});

await download.start();

console.log('done');
