import { UserRecord } from '../types';
import { fetchJson } from './request';

export const getUserRecords = async (
  offset = 0,
  limit = 50,
): Promise<UserRecord[]> => {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  const data = await fetchJson<{ records: { partition_key: string; clustering_key: string | null; value: string }[] }>(`/data/records?${params.toString()}`);
  return data.records.map(r => ({
    partitionKey: r.partition_key,
    clusteringKey: r.clustering_key ?? undefined,
    value: r.value,
  }));
};

export const saveUserRecord = async (record: UserRecord): Promise<UserRecord> => {
  if (record.clusteringKey) {
    const path = `/data/records/${encodeURIComponent(record.partitionKey)}/${encodeURIComponent(record.clusteringKey)}?value=${encodeURIComponent(record.value)}`;
    await fetchJson<{ status: string }>(path, { method: 'PUT' });
  } else {
    await fetchJson<{ status: string }>('/data/records', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ partitionKey: record.partitionKey, value: record.value }),
    });
  }
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
