import { test } from 'node:test';
import assert from 'node:assert';
import { MBTilesOffline } from '../index.js';

test('MBTilesOffline - Instantiation', async (t) => {
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

test('MBTilesOffline - Coordinate Conversions', async (t) => {
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

test('MBTilesOffline - Coverage', async (t) => {
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
