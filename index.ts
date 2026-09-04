import type { BBox } from 'geojson';
import { PromisePool } from '@supercharge/promise-pool'
import EventEmitter from 'node:events';
import { DatabaseSync } from 'node:sqlite';
import sharp from 'sharp';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const MAX_RETRIES = 4; // Per tile
const INITIAL_DELAY_MS = 250;  // ms
const MAX_LAT = 85.05112878;

export interface Tile {
    z: number;
    x: number;
    y: number;
}

export interface PixelRect {
    left: number;
    top: number;
    width: number;
    height: number;
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

    /** Make pixels outside of bounds transparent (Default: true) */
    clip?: boolean;
};

export class MBTilesOffline extends EventEmitter {
    bounds: BBox;
    minzoom: number;
    maxzoom: number;
    url: string;
    output: string;
    concurrency: number;
    clip: boolean;

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
        this.clip = options.clip !== false;
    }

    async start(): Promise<void> {
        const db = new DatabaseSync(this.output);

        let total = 0;
        let progress = 0;

        for (let zoom = this.minzoom; zoom <= this.maxzoom; zoom++) {
            const [minLon, minLat, maxLon, maxLat] = this.bounds;
            const startX = this.lonToTileX(minLon, zoom);
            const endX = this.lonToTileX(maxLon, zoom);
            const startY = this.latToTileY(maxLat, zoom);
            const endY = this.latToTileY(minLat, zoom);

            total += Math.abs((endX - startX + 1) * (endY - startY + 1));
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

                        let data = await this.downloadTile(tile);

                        if (data && this.clip) {
                            data = await this.clipTile(tile, data);
                        }

                        if (data) {
                            stmt.run(tile.z, tile.x, tmsY, data);
                            this.emit('progress', ++progress);
                        } else {
                            this.emit('progress', ++progress);
                            throw new Error('Failed to download data for tile: ' + JSON.stringify(tile));
                        }
                    } catch (err) {
                        this.emit('error', err);
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
            } catch (error: unknown) {
                const msg = error instanceof Error ? error.message : String(error);
                console.error(`Attempt ${attempt} failed for ${url} with error: ${msg}`);
            }

            if (attempt === MAX_RETRIES) {
                break;
            }

            const delay = INITIAL_DELAY_MS * Math.pow(2, attempt - 1);

            await sleep(delay);
        }

        return null;
    }

    /**
     * Make any portion of the tile image that falls outside of the configured
     * bounds transparent so partial edge tiles only display the requested area.
     * Tiles fully inside the bounds are returned untouched.
     */
    async clipTile(tile: Tile, data: Buffer): Promise<Buffer> {
        const image = sharp(data);
        const meta = await image.metadata();
        const size = meta.width;

        if (!size || !meta.height) return data;

        const rect = this.clipRect(tile, this.bounds, size);

        if (!rect) {
            return await sharp({
                create: {
                    width: size,
                    height: meta.height,
                    channels: 4,
                    background: { r: 0, g: 0, b: 0, alpha: 0 }
                }
            }).png().toBuffer();
        }

        if (rect.left === 0 && rect.top === 0 && rect.width === size && rect.height === meta.height) {
            return data;
        }

        return await image
            .ensureAlpha()
            .extract(rect)
            .extend({
                left: rect.left,
                top: rect.top,
                right: size - rect.left - rect.width,
                bottom: meta.height - rect.top - rect.height,
                background: { r: 0, g: 0, b: 0, alpha: 0 }
            })
            .png()
            .toBuffer();
    }

    /**
     * Pixel rectangle within a tile of the given size that intersects bounds
     * @returns The intersecting rectangle or null if the tile is entirely outside bounds
     */
    clipRect(tile: Tile, bounds: BBox, size: number): PixelRect | null {
        const [minLon, minLat, maxLon, maxLat] = bounds;

        const left = Math.max(0, Math.floor((this.lonToX(minLon, tile.z) - tile.x) * size));
        const right = Math.min(size, Math.ceil((this.lonToX(maxLon, tile.z) - tile.x) * size));
        const top = Math.max(0, Math.floor((this.latToY(maxLat, tile.z) - tile.y) * size));
        const bottom = Math.min(size, Math.ceil((this.latToY(minLat, tile.z) - tile.y) * size));

        if (right <= left || bottom <= top) return null;

        return {
            left,
            top,
            width: right - left,
            height: bottom - top
        };
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
        return Math.floor(this.lonToX(lon, zoom));
    }

    /**
     * Converts longitude to a fractional tile X coordinate.
     */
    lonToX(lon: number, zoom: number): number {
        return ((lon + 180) / 360) * Math.pow(2, zoom);
    }

    /**
     * Converts latitude to a fractional tile Y coordinate.
     */
    latToY(lat: number, zoom: number): number {
        const rad = (Math.max(-MAX_LAT, Math.min(MAX_LAT, lat)) * Math.PI) / 180;
        return ((1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2) * Math.pow(2, zoom);
    }

    /**
     * Converts latitude to tile Y coordinate.
     * @param lat - Latitude.
     * @param zoom - Zoom level.
     * @returns Tile Y coordinate.
     */
    latToTileY(lat: number, zoom: number): number {
        return Math.floor(this.latToY(lat, zoom));
    }
}
