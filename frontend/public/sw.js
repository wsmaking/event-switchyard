const VERSION = 'switchyard-pwa-v2';
const SHELL_CACHE = `${VERSION}-shell`;
const RUNTIME_CACHE = `${VERSION}-runtime`;
const SCOPE_URL = new URL(self.registration.scope);
const SCOPE_PATH = SCOPE_URL.pathname.endsWith('/') ? SCOPE_URL.pathname.slice(0, -1) : SCOPE_URL.pathname;
const SHELL_URLS = [
  scopedPath('/'),
  scopedPath('/mobile'),
  scopedPath('/manifest.webmanifest'),
  scopedPath('/app-icon.svg'),
  scopedPath('/app-icon-192.png'),
  scopedPath('/app-icon-512.png'),
  scopedPath('/apple-touch-icon.png'),
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then((cache) => cache.addAll(SHELL_URLS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== SHELL_CACHE && key !== RUNTIME_CACHE)
          .map((key) => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') {
    return;
  }

  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) {
    return;
  }

  const relativePath = toRelativePath(url.pathname);

  if (relativePath.startsWith('/api/')) {
    event.respondWith(fetch(event.request));
    return;
  }

  if (event.request.mode === 'navigate') {
    event.respondWith(networkFirstDocument(event.request, relativePath));
    return;
  }

  event.respondWith(staleWhileRevalidate(event.request));
});

async function networkFirstDocument(request, relativePath) {
  try {
    const response = await fetch(request);
    const cache = await caches.open(RUNTIME_CACHE);
    cache.put(request, response.clone());
    return response;
  } catch (_error) {
    const cache = await caches.open(RUNTIME_CACHE);
    const cached = await cache.match(request);
    if (cached) {
      return cached;
    }
    if (relativePath.startsWith('/mobile')) {
      return (await caches.match(scopedPath('/mobile'))) || caches.match(scopedPath('/'));
    }
    return caches.match(scopedPath('/'));
  }
}

async function staleWhileRevalidate(request) {
  const cache = await caches.open(RUNTIME_CACHE);
  const cached = await cache.match(request);
  const fetchPromise = fetch(request)
    .then((response) => {
      if (response.ok) {
        cache.put(request, response.clone());
      }
      return response;
    })
    .catch(() => cached);

  return cached || fetchPromise;
}

function scopedPath(path) {
  if (SCOPE_PATH === '') {
    return path;
  }
  return path === '/' ? `${SCOPE_PATH}/` : `${SCOPE_PATH}${path}`;
}

function toRelativePath(pathname) {
  if (SCOPE_PATH !== '' && pathname.startsWith(SCOPE_PATH)) {
    const sliced = pathname.slice(SCOPE_PATH.length);
    return sliced === '' ? '/' : sliced;
  }
  return pathname;
}
