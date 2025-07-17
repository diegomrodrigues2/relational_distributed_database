export const API_BASE =
  (typeof import.meta !== 'undefined' && (import.meta as any).env?.VITE_API_BASE) ||
  'http://localhost:8000';

export const fetchJson = async <T>(
  path: string,
  options?: RequestInit,
): Promise<T> => {
  const resp = await fetch(`${API_BASE}${path}`, options);
  if (!resp.ok) {
    let message = `Request failed: ${resp.status}`;
    try {
      const data = await resp.json();
      if (data && data.detail) message = data.detail;
    } catch {
      // ignore json parse errors and fall back to status message
    }
    throw new Error(message);
  }
  if (
    resp.status === 204 ||
    (resp.headers && 'get' in resp.headers && resp.headers.get('content-length') === '0')
  ) {
    return {} as T;
  }
  return resp.json() as Promise<T>;
};
