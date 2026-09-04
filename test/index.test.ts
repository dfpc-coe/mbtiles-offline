import { test } from 'node:test';
import assert from 'node:assert';
import sharp from 'sharp';
import { MBTilesOffline } from '../index.js';

test('MBTilesOffline - Instantiation', async () => {
    const config = {
        name: 'Test',
        version: '1.0.0',
        description: 'Test Description',
        bounds: [-180, -90, 180, 90] as [number, number, number, number],
        minzoom: 0,
        maxzoom: 2,
        url: 'http://localhost:3000/{z}/{x}/{y}.png',
        output: '/tmp/test.mbtiles'
    };
    const mbtiles = new MBTilesOffline(config);

    assert.equal(mbtiles.name, 'Test');
    assert.equal(mbtiles.minzoom, 0);
    assert.equal(mbtiles.concurrency, 10); // Default check
});

test('MBTilesOffline - Coordinate Conversions', async () => {
    const config = {
        name: 'Test',
        version: '1.0.0',
        description: 'Test Description',
        bounds: [-180, -90, 180, 90] as [number, number, number, number],
        minzoom: 0,
        maxzoom: 0,
        url: 'http://localhost',
        output: '/tmp/test.mbtiles'
    };
    const mbtiles = new MBTilesOffline(config);

    // Zoom 0, entire world is 1 tile (0,0) usually.
    // lonToTileX(-180, 0) -> 0
    assert.equal(mbtiles.lonToTileX(-180, 0), 0);
    
    // latToTileY matches typical web mercator projection logic
    // At zoom 0, 85.0511 lat is approx top edge -> 0
    const y = mbtiles.latToTileY(0, 0);
    // Rough check
    assert.ok(y >= 0);
});

test('MBTilesOffline - Coverage', async () => {
    const config = {
        name: 'Test',
        version: '1.0.0',
        description: 'Test Description',
        bounds: [-180, -90, 180, 90] as [number, number, number, number],
        minzoom: 0,
        maxzoom: 0,
        url: 'http://localhost',
        output: '/tmp/test.mbtiles'
    };
    const mbtiles = new MBTilesOffline(config);

    const tiles = [];
    for (const tile of mbtiles.coverage(0, [-180, -85, 180, 85])) {
        tiles.push(tile);
    }
    
    // Should contain at least 0/0/0
    assert.ok(tiles.some(t => t.z === 0 && t.x === 0 && t.y === 0));
});

test('MBTilesOffline - clipRect', async () => {
    const mbtiles = new MBTilesOffline({
        bounds: [-90, 0, 90, 66.51326] as [number, number, number, number],
        minzoom: 0,
        maxzoom: 2,
        url: 'http://localhost',
        output: '/tmp/test.mbtiles'
    });

    // Zoom 0: bounds cover the middle half horizontally and 1/4 of the height
    assert.deepEqual(mbtiles.clipRect({ z: 0, x: 0, y: 0 }, mbtiles.bounds, 256), {
        left: 64,
        top: 64,
        width: 128,
        height: 64
    });

    // Zoom 2: tile 1/1 is entirely inside the bounds
    assert.deepEqual(mbtiles.clipRect({ z: 2, x: 1, y: 1 }, mbtiles.bounds, 256), {
        left: 0,
        top: 0,
        width: 256,
        height: 256
    });

    // Zoom 2: tile 0/0 is entirely outside the bounds
    assert.equal(mbtiles.clipRect({ z: 2, x: 0, y: 0 }, mbtiles.bounds, 256), null);
});

test('MBTilesOffline - clipTile', async () => {
    const mbtiles = new MBTilesOffline({
        bounds: [-90, 0, 90, 66.51326] as [number, number, number, number],
        minzoom: 0,
        maxzoom: 2,
        url: 'http://localhost',
        output: '/tmp/test.mbtiles'
    });

    const red = await sharp({
        create: { width: 256, height: 256, channels: 3, background: { r: 255, g: 0, b: 0 } }
    }).png().toBuffer();

    // Fully inside: original buffer is returned untouched
    assert.equal(await mbtiles.clipTile({ z: 2, x: 1, y: 1 }, red), red);

    const clipped = await mbtiles.clipTile({ z: 0, x: 0, y: 0 }, red);
    assert.notEqual(clipped, red);

    const { data, info } = await sharp(clipped).raw().toBuffer({ resolveWithObject: true });
    assert.equal(info.width, 256);
    assert.equal(info.height, 256);
    assert.equal(info.channels, 4);

    const px = (x: number, y: number) => Array.from(data.subarray((y * 256 + x) * 4, (y * 256 + x) * 4 + 4));

    assert.deepEqual(px(0, 0), [0, 0, 0, 0]);
    assert.deepEqual(px(128, 32), [0, 0, 0, 0]);
    assert.deepEqual(px(32, 96), [0, 0, 0, 0]);
    assert.deepEqual(px(128, 96), [255, 0, 0, 255]);
    assert.deepEqual(px(64, 64), [255, 0, 0, 255]);
    assert.deepEqual(px(191, 127), [255, 0, 0, 255]);
    assert.deepEqual(px(192, 128), [0, 0, 0, 0]);

    // Fully outside: transparent tile
    const empty = await mbtiles.clipTile({ z: 2, x: 0, y: 0 }, red);
    const raw = await sharp(empty).raw().toBuffer({ resolveWithObject: true });
    assert.equal(raw.info.channels, 4);
    assert.ok(raw.data.every((v) => v === 0));
});
