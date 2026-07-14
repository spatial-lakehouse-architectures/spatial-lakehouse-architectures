/**
 * Content-hash first-party CSS/JS assets so cache-busting query strings change
 * whenever a source file changes. This prevents the service worker from serving
 * stale styling/scripts after a deploy.
 *
 * Each key maps to a 10-char slice of the sha1 hash of the source file's
 * contents. Referenced in templates as `?v={{ assets.<key> }}`.
 */
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const ASSET_DIR = path.join(__dirname, "..", "assets");

// Map of export key -> source file (relative to src/assets). Only FIRST-PARTY
// (incl. local vendor) css/js files that are precached/served under fixed names.
const FILES = {
  site_css: "css/site.css",
  katex_css: "css/katex.min.css",
  site_js: "js/site.js",
};

function hashFile(relPath) {
  const abs = path.join(ASSET_DIR, relPath);
  const buf = fs.readFileSync(abs);
  return crypto.createHash("sha1").update(buf).digest("hex").slice(0, 10);
}

const assets = {};
for (const [key, rel] of Object.entries(FILES)) {
  assets[key] = hashFile(rel);
}

module.exports = assets;
