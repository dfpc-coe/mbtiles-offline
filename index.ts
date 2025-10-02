import type { BBox } from 'geojson';
import { PromisePool } from '@supercharge/promise-pool'
import EventEmitter from 'node:events';
import { fetch } from 'undici';
import { DatabaseSync } from 'node:sqlite';
import fsp from 'node:fs/promises';
import path from 'node:path'

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const MAX_RETRIES = 4; // Per tile
const INITIAL_DELAY_MS = 250;  // ms

export interface Tile {
    z: number;
    x: number;
    y: number;
}

export interface Config {
    bounds: BBox;
    minzoom: number;
    maxzoom: number;
    url: string;
    output: string;

    name?: string;
    version?: string;
    description?: string;

    concurrency?: number;
};

export class MBTilesOffline extends EventEmitter {
    bounds: BBox;
    minzoom: number;
    maxzoom: number;
    url: string;
    output: string;
    concurrency: number;

    name: string;
    version: string;
    description: string;

    constructor(options: Config) {
        super();

        this.bounds = options.bounds;
        this.minzoom = options.minzoom;
        this.maxzoom = options.maxzoom;
        this.url = options.url;
        this.output = options.output;

        this.name = options.name || 'Default Tileset';
        this.version = options.version || '1.0.0';
        this.description = options.description || '';

        this.concurrency = options.concurrency || 10;

    }

    async start(): Promise<MBTilesOffline> {
        const db = new DatabaseSync(this.output);

        let total = 0;
        let progress = 0;
        for (let zoom = this.minzoom; zoom <= this.maxzoom; zoom++) {
            for (const tile of this.coverage(zoom, this.bounds)) {
                total++;
            }
        }

        this.emit('total', total);

        await db.exec(`
            CREATE TABLE IF NOT EXISTS metadata (
                name TEXT,
                value TEXT
            );
        `);

        db.exec(`
            CREATE TABLE IF NOT EXISTS tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB
            );
        `);

        db.exec(`
            CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (
                zoom_level,
                tile_column,
                tile_row
            );
        `);

        db.exec(`INSERT INTO metadata (name, value) VALUES ('name', '${this.name}')`);
        db.exec(`INSERT INTO metadata (name, value) VALUES ('version', '${this.version}')`);
        db.exec(`INSERT INTO metadata (name, value) VALUES ('description', '${this.description}')`);
        db.exec(`INSERT INTO metadata (name, value) VALUES ('format', 'png')`);
        db.exec(`INSERT INTO metadata (name, value) VALUES ('minzoom', '${this.minzoom}')`);
        db.exec(`INSERT INTO metadata (name, value) VALUES ('maxzoom', '${this.maxzoom}')`);
        db.exec(`INSERT INTO metadata (name, value) VALUES ('bounds', '${this.bounds.join(',')}')`);

        const check = db.prepare(
            'SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?'
        );

        const stmt = db.prepare(
            'INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)'
        );

        const pool = new Array();




        for (let zoom = this.minzoom; zoom <= this.maxzoom; zoom++) {
            await PromisePool
                .withConcurrency(this.concurrency)
                .for(this.coverage(zoom, this.bounds))
                .process(async (tile) => {
                    try {
                        // MBTiles spec uses TMS tiling scheme, which has a flipped Y-axis
                        // compared to the ZXY scheme used by most web maps (like OSM).
                        const tmsY = (1 << tile.z) - 1 - tile.y;

                        const checkResult = check.get(tile.z, tile.x, tmsY);

                        if (checkResult && checkResult.tile_data) {
                            this.emit('progress', ++progress);
                            return;
                        }

                        const data = await this.downloadTile(tile);

                        if (data) {
                            stmt.run(tile.z, tile.x, tmsY, data);
                            this.emit('progress', ++progress);
                        } else {
                            this.emit('progress', ++progress);
                            this.emit('error', new Error('Failed to download data for tile: ' + JSON.stringify(tile)));
                        }
                    } catch (err) {
                        console.error(err);
                    }
                });
        }

        db.close();
    }

    async downloadTile(tile: Tile): Promise<Buffer | null> {
        const url = this.url.replace('{z}', String(tile.z)).replace('{x}', String(tile.x)).replace('{y}', String(tile.y));

        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                const res = await fetch(url);

                if (res.ok) return Buffer.from(await res.arrayBuffer());

                if (res.status === 404) {
                    console.warn(`Tile not found (404), no retry needed: ${url}`);
                    return null;
                }
            } catch (err) {
                console.error(`Attempt ${attempt} failed for ${url} with error: ${error.message}`);
            }

            if (attempt === MAX_RETRIES) {
                break;
            }

            const delay = INITIAL_DELAY_MS * Math.pow(2, attempt - 1);

            await sleep(delay);
        }

        return null;
    }

    *coverage(
        zoom: number,
        bounds: BBox
    ): Generator<Tile> {
        const [minLon, minLat, maxLon, maxLat] = bounds;

        const startX = this.lonToTileX(minLon, zoom);
        const endX = this.lonToTileX(maxLon, zoom);
        const startY = this.latToTileY(maxLat, zoom);
        const endY = this.latToTileY(minLat, zoom);

        for (let x = startX; x <= endX; x++) {
            for (let y = startY; y <= endY; y++) {
                yield { z: zoom, x, y };
            }
        }
    }

    /**
     * Converts longitude to tile X coordinate.
     * @param lon - Longitude.
     * @param zoom - Zoom level.
     * @returns Tile X coordinate.
     */
    lonToTileX(lon: number, zoom: number): number {
        return Math.floor(((lon + 180) / 360) * Math.pow(2, zoom));
    }

    /**
     * Converts latitude to tile Y coordinate.
     * @param lat - Latitude.
     * @param zoom - Zoom level.
     * @returns Tile Y coordinate.
     */
    latToTileY(lat: number, zoom: number): number {
        return Math.floor(
            ((1 -
              Math.log(
                  Math.tan((lat * Math.PI) / 180) +
                      1 / Math.cos((lat * Math.PI) / 180)
            ) /
                Math.PI) /
                2) *
                Math.pow(2, zoom)
        );
    }
}
