export const API_BASE = 'http://localhost:8000';

export const fetchJson = async <T>(path: string, options?: RequestInit): Promise<T> => {
  const resp = await fetch(`${API_BASE}${path}`, options);
  if (!resp.ok) {
    throw new Error(`Request failed: ${resp.status}`);
  }
  if (
    resp.status === 204 ||
    (resp.headers && 'get' in resp.headers && resp.headers.get('content-length') === '0')
  ) {
    return {} as T;
  }
  return resp.json() as Promise<T>;
};
