import { UserRecord } from '../types';

const API_BASE = 'http://localhost:8000';

const fetchJson = async <T>(path: string, options?: RequestInit): Promise<T> => {
  const resp = await fetch(`${API_BASE}${path}`, options);
  if (!resp.ok) {
    throw new Error(`Request failed: ${resp.status}`);
  }
  if (resp.headers.get('content-length') === '0' || resp.status === 204) {
    return {} as T;
  }
  return resp.json() as Promise<T>;
};

export const getUserRecords = async (): Promise<UserRecord[]> => {
  const data = await fetchJson<{ records: { partition_key: string; clustering_key: string; value: string }[] }>('/data/records');
  return data.records.map(r => ({
    partitionKey: r.partition_key,
    clusteringKey: r.clustering_key,
    value: r.value,
  }));
};

export const saveUserRecord = async (record: UserRecord): Promise<UserRecord> => {
  const path = `/data/records/${encodeURIComponent(record.partitionKey)}/${encodeURIComponent(record.clusteringKey)}?value=${encodeURIComponent(record.value)}`;
  await fetchJson<{ status: string }>(path, { method: 'PUT' });
  return record;
};

export const deleteUserRecord = async (partitionKey: string, clusteringKey: string): Promise<void> => {
  const path = `/data/records/${encodeURIComponent(partitionKey)}/${encodeURIComponent(clusteringKey)}`;
  await fetchJson<{ status: string }>(path, { method: 'DELETE' });
};

export const scanRange = async (
  partitionKey: string,
  startCk: string,
  endCk: string,
): Promise<UserRecord[]> => {
  const params = new URLSearchParams({ partition_key: partitionKey, start_ck: startCk, end_ck: endCk });
  const data = await fetchJson<{ items: { clustering_key: string; value: string }[] }>(`/data/records/scan_range?${params.toString()}`);
  return data.items.map(i => ({
    partitionKey,
    clusteringKey: i.clustering_key,
    value: i.value,
  }));
};
