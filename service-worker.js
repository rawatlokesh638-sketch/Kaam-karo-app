// Service Worker for KaamKaro App
const CACHE_NAME = 'kaamkaro-v4.0';
const urlsToCache = [
  '/',
  '/index.html',
  '/admin.html',
  '/manifest.json'
];

// Install event
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        console.log('📦 Caching app shell');
        return cache.addAll(urlsToCache);
      })
      .then(() => self.skipWaiting())
  );
});

// Activate event
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheName !== CACHE_NAME) {
            console.log('🗑️ Deleting old cache:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    }).then(() => self.clients.claim())
  );
});

// Fetch event
self.addEventListener('fetch', event => {
  // Skip API calls
  if (event.request.url.includes('/api/')) {
    return;
  }

  event.respondWith(
    caches.match(event.request)
      .then(response => {
        // Return cached version or fetch new
        return response || fetch(event.request)
          .then(response => {
            // Cache new resources
            if (!response || response.status !== 200 || response.type !== 'basic') {
              return response;
            }

            const responseToCache = response.clone();
            caches.open(CACHE_NAME)
              .then(cache => {
                cache.put(event.request, responseToCache);
              });

            return response;
          })
          .catch(() => {
            // Fallback for offline
            if (event.request.mode === 'navigate') {
              return caches.match('/');
            }
          });
      })
  );
});

// Push notification event
self.addEventListener('push', event => {
  const options = {
    body: event.data ? event.data.text() : 'New task available!',
    icon: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/icons/cash-stack.svg',
    badge: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/icons/cash-stack.svg',
    vibrate: [100, 50, 100],
    data: {
      dateOfArrival: Date.now(),
      primaryKey: 1
    },
    actions: [
      {
        action: 'explore',
        title: 'Start Earning',
        icon: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/icons/play-circle.svg'
      },
      {
        action: 'close',
        title: 'Close',
        icon: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/icons/x-circle.svg'
      }
    ]
  };

  event.waitUntil(
    self.registration.showNotification('💰 KaamKaro', options)
  );
});

// Notification click event
self.addEventListener('notificationclick', event => {
  event.notification.close();

  if (event.action === 'explore') {
    event.waitUntil(
      clients.openWindow('/')
    );
  } else {
    event.waitUntil(
      clients.openWindow('/')
    );
  }
});

// Background sync for offline tasks
self.addEventListener('sync', event => {
  if (event.tag === 'sync-tasks') {
    event.waitUntil(syncTasks());
  }
});

async function syncTasks() {
  // Sync pending tasks when online
  console.log('🔄 Syncing tasks...');
}