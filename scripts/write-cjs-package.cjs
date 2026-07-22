const { mkdirSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");

const dir = join(__dirname, "..", "dist-cjs");
mkdirSync(dir, { recursive: true });
writeFileSync(join(dir, "package.json"), '{"type":"commonjs"}\n');
