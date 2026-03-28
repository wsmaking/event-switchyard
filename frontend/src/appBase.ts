const RAW_BASE_URL = import.meta.env.BASE_URL || '/';

export function appBasePath() {
  if (RAW_BASE_URL === '/') {
    return '';
  }
  return RAW_BASE_URL.endsWith('/') ? RAW_BASE_URL.slice(0, -1) : RAW_BASE_URL;
}

export function toAppPath(browserPath: string) {
  const basePath = appBasePath();
  if (!basePath) {
    return normalizePath(browserPath);
  }
  if (browserPath === basePath) {
    return '/';
  }
  if (browserPath.startsWith(`${basePath}/`)) {
    return normalizePath(browserPath.slice(basePath.length));
  }
  return normalizePath(browserPath);
}

export function toBrowserPath(appPath: string) {
  const normalized = normalizePath(appPath);
  if (isLocalBundleRuntime()) {
    return `#${normalized}`;
  }
  const basePath = appBasePath();
  if (!basePath) {
    return normalized;
  }
  return normalized === '/' ? basePath : `${basePath}${normalized}`;
}

export function isMobileAppPath(browserPath: string) {
  return toAppPath(browserPath).startsWith('/mobile');
}

export function isLocalBundleRuntime() {
  if (typeof window === 'undefined') {
    return false;
  }
  const protocol = window.location.protocol;
  return protocol === 'file:' || protocol === 'appbundle:';
}

export function isMobileRuntime() {
  if (isLocalBundleRuntime()) {
    return true;
  }
  return typeof window !== 'undefined' && isMobileAppPath(window.location.pathname || '/');
}

export function readCurrentAppPath() {
  if (typeof window === 'undefined') {
    return '/';
  }
  if (isLocalBundleRuntime()) {
    const hashPath = window.location.hash.replace(/^#/, '');
    return normalizePath(hashPath || '/mobile');
  }
  return toAppPath(window.location.pathname || '/');
}

function normalizePath(path: string) {
  if (!path || path === '') {
    return '/';
  }
  return path.startsWith('/') ? path : `/${path}`;
}
