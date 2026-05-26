/* Spatial Lakehouse Architectures — service worker */
const VERSION = "v1";
const PRECACHE = `sla-precache-${VERSION}`;
const RUNTIME = `sla-runtime-${VERSION}`;

const PRECACHE_URLS = [
  "/",
  "/offline/",
  "/assets/css/site.css",
  "/assets/js/site.js",
  "/assets/icons/favicon.svg",
  "/assets/icons/icon-192.svg",
  "/assets/icons/icon-512.svg",
  "/manifest.webmanifest",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(PRECACHE)
      .then((cache) => cache.addAll(PRECACHE_URLS.map((u) => new Request(u, { cache: "reload" }))))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  const expected = new Set([PRECACHE, RUNTIME]);
  event.waitUntil(
    (async () => {
      if (self.registration.navigationPreload) {
        try { await self.registration.navigationPreload.enable(); } catch (_) {}
      }
      const keys = await caches.keys();
      await Promise.all(keys.filter((k) => !expected.has(k)).map((k) => caches.delete(k)));
      await self.clients.claim();
    })()
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  // Navigation requests: network-first, fall back to cache, then offline page.
  if (req.mode === "navigate") {
    event.respondWith(
      (async () => {
        try {
          const preload = await event.preloadResponse;
          if (preload) return preload;
          const fresh = await fetch(req);
          const cache = await caches.open(RUNTIME);
          cache.put(req, fresh.clone());
          return fresh;
        } catch (_) {
          const cached = await caches.match(req);
          if (cached) return cached;
          return (await caches.match("/offline/")) || new Response("Offline", { status: 503 });
        }
      })()
    );
    return;
  }

  // Static assets: stale-while-revalidate.
  if (url.pathname.startsWith("/assets/") || url.pathname.endsWith(".webmanifest")) {
    event.respondWith(
      caches.open(RUNTIME).then(async (cache) => {
        const cached = await cache.match(req);
        const network = fetch(req)
          .then((res) => {
            if (res && res.ok) cache.put(req, res.clone());
            return res;
          })
          .catch(() => cached);
        return cached || network;
      })
    );
  }
});
