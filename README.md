<h1 align="center">@tak-ps/mbtiles-offline</h1>

Download Web maps into an offline MBTiles database.

## Usage

```javascript
import { MBTilesOffline } from '@tak-ps/mbtiles-offline';

const download = new MBTilesOffline({
    name: 'My Map',
    description: 'Offline satellite imagery',
    version: '1.0.0',
    bounds: [-108.39, 38.60, -108.30, 38.65],
    minzoom: 0,
    maxzoom: 14,
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    output: './satellite.mbtiles',
    concurrency: 50
});

download.on('total', (total) => {
    console.log(`Downloading ${total} tiles...`);
});

download.on('progress', (current) => {
    console.log(`Progress: ${current}`);
});

download.on('error', (err) => {
    console.error(err);
});

await download.start();
```

## Configuration

The `MBTilesOffline` constructor accepts the following options:

| Option | Type | Description |
|---|---|---|
| `bounds` | `[number, number, number, number]` | **Required** GeoJSON BBox `[minX, minY, maxX, maxY]` |
| `minzoom` | `number` | **Required** Minimum Zoom Level |
| `maxzoom` | `number` | **Required** Maximum Zoom Level |
| `url` | `string` | **Required** Tile URL Template. Must include `{z}`, `{x}`, `{y}` |
| `output` | `string` | **Required** Path to write the MBTiles database |
| `name` | `string` | Name of the tileset (metadata) |
| `description` | `string` | Description of the tileset (metadata) |
| `version` | `string` | Version of the tileset (metadata) |
| `concurrency` | `number` | Number of concurrent requests (Default: `Infinity`) |
