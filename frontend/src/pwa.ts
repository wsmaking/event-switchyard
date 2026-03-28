export async function registerServiceWorker() {
  if (!('serviceWorker' in navigator) || !import.meta.env.PROD) {
    return;
  }
  try {
    await navigator.serviceWorker.register('/sw.js');
  } catch (error) {
    console.warn('service_worker_registration_failed', error);
  }
}

export function isStandaloneDisplay() {
  if (typeof window === 'undefined') {
    return false;
  }
  return window.matchMedia('(display-mode: standalone)').matches || Boolean((window.navigator as Navigator & { standalone?: boolean }).standalone);
}
