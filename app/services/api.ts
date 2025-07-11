import {
  Node,
  NodeStatus,
  Partition,
  MetricPoint,
  ClusterConfig,
  HotspotInfo,
  ReplicationStatus,
  WALEntry,
  StorageEntry,
  SSTableInfo,
  TransactionInfo,
} from '../types';
import { fetchJson } from './request';

export const getNodes = async (): Promise<Node[]> => {
  const data = await fetchJson<{ nodes: any[] }>('/cluster/nodes');
  return data.nodes.map(n => ({
    id: n.node_id,
    address: `${n.host}:${n.port}`,
    status: (n.status || 'DEAD').toUpperCase() as NodeStatus,
    uptime: String(n.uptime ?? '0'),
    cpuUsage: n.cpu ?? 0,
    memoryUsage: n.memory ?? 0,
    diskUsage: n.disk ?? 0,
    dataLoad: 0,
    replicationLogSize: n.replication_log_size ?? 0,
    hintsCount: n.hints_count ?? 0,
  }));
};

export const getPartitions = async (): Promise<Partition[]> => {
  const data = await fetchJson<{ partitions: any[] }>('/cluster/partitions');
  return data.partitions.map(p => ({
    id: p.id.toString(),
    primaryNodeId: p.node,
    replicaNodeIds: [],
    keyRange: [p.key_range?.[0] ?? '', p.key_range?.[1] ?? ''],
    size: 0,
    itemCount: p.items ?? 0,
    operationCount: p.ops ?? 0,
  }));
};

export const getClusterConfig = async (): Promise<ClusterConfig> => {
  const data = await fetchJson<any>('/cluster/config');
  return {
    consistencyMode: data.consistency_mode,
    replicationFactor: data.replication_factor,
    writeQuorum: data.write_quorum,
    readQuorum: data.read_quorum,
    partitionStrategy: data.partition_strategy,
    partitionsPerNode: data.partitions_per_node,
    topology: 'all-to-all',
    maxTransferRate: 0,
    antiEntropyInterval: 0,
    maxBatchSize: 0,
    heartbeatInterval: 0,
    heartbeatTimeout: 0,
    hintedHandoffInterval: 0,
  };
};

export const getDashboardTimeSeriesMetrics = async (): Promise<{
  latency: MetricPoint[];
  throughput: MetricPoint[];
  replicationLogTotal: MetricPoint[];
  hintsTotal: MetricPoint[];
}> => {
  const data = await fetchJson<any>('/cluster/metrics/time_series');
  const t = new Date().toLocaleTimeString();
  return {
    latency: (data.latency_ms || []).map((v: number, i: number) => ({ time: `${t}_${i}`, value: v })),
    throughput: [{ time: t, value: data.throughput ?? 0 }],
    replicationLogTotal: [{ time: t, value: data.replication_log_size ?? 0 }],
    hintsTotal: [{ time: t, value: data.hints_count ?? 0 }],
  };
};

export const getHotspots = async (): Promise<HotspotInfo> => {
  const data = await fetchJson<any>('/cluster/hotspots');
  return {
    hotPartitions: (data.hot_partitions || []).map((p: any) => ({
      id: String(p.id),
      operationCount: p.operation_count ?? 0,
      averageOps: p.average_ops ?? 0,
    })),
    hotKeys: (data.hot_keys || []).map((k: any) => ({
      key: k.key,
      frequency: k.frequency ?? 0,
    })),
  };
};

export const getReplicationStatus = async (): Promise<ReplicationStatus[]> => {
  const nodes = await getNodes();
  const statuses = await Promise.all(
    nodes.map(async n => {
      try {
        const data = await fetchJson<any>(`/nodes/${n.id}/replication_status`);
        return {
          nodeId: n.id,
          replicationLogSize: n.replicationLogSize,
          lastSeen: data.last_seen ?? {},
          hints: data.hints ?? {},
        } as ReplicationStatus;
      } catch {
        return {
          nodeId: n.id,
          replicationLogSize: n.replicationLogSize,
          lastSeen: {},
          hints: {},
        } as ReplicationStatus;
      }
    })
  );
  return statuses;
};


export const addNode = async (): Promise<string> => {
  const data = await fetchJson<{ status: string; node_id: string }>(
    '/cluster/actions/add_node',
    { method: 'POST' },
  );
  return data.node_id;
};

export const removeNode = async (nodeId: string): Promise<string> => {
  await fetchJson<{ status: string }>(
    `/cluster/actions/remove_node/${encodeURIComponent(nodeId)}`,
    { method: 'DELETE' },
  );
  return nodeId;
};

export const stopNode = async (nodeId: string): Promise<Node> => {
  await fetchJson<{ status: string }>(
    `/nodes/${encodeURIComponent(nodeId)}/stop`,
    { method: 'POST' },
  );
  const nodes = await getNodes();
  const node = nodes.find(n => n.id === nodeId)!;
  return node;
};

export const startNode = async (nodeId: string): Promise<Node> => {
  await fetchJson<{ status: string }>(
    `/nodes/${encodeURIComponent(nodeId)}/start`,
    { method: 'POST' },
  );
  const nodes = await getNodes();
  const node = nodes.find(n => n.id === nodeId)!;
  return node;
};

export const getWalEntries = async (
  nodeId: string,
  offset = 0,
  limit = 50,
): Promise<WALEntry[]> => {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  const data = await fetchJson<{ entries: any[] }>(
    `/nodes/${encodeURIComponent(nodeId)}/wal?${params.toString()}`,
  );
  return (data.entries || []).map(e => ({
    type: e.type,
    key: e.key,
    value: e.value ?? undefined,
    vectorClock: e.vector_clock ?? {},
  }));
};

export const getMemtableEntries = async (
  nodeId: string,
  offset = 0,
  limit = 50,
): Promise<StorageEntry[]> => {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  const data = await fetchJson<{ entries: any[] }>(
    `/nodes/${encodeURIComponent(nodeId)}/memtable?${params.toString()}`,
  );
  return (data.entries || []).map(e => ({
    key: e.key,
    value: e.value,
    vectorClock: e.vector_clock ?? {},
  }));
};

export const getSstables = async (nodeId: string): Promise<SSTableInfo[]> => {
  const data = await fetchJson<{ tables: any[] }>(
    `/nodes/${encodeURIComponent(nodeId)}/sstables`,
  );
  return (data.tables || []).map(t => ({
    id: t.id,
    level: t.level ?? 0,
    size: t.size ?? 0,
    itemCount: t.item_count ?? 0,
    keyRange: [t.key_range?.[0] ?? '', t.key_range?.[1] ?? ''],
  }));
};

export const getSstableEntries = async (
  nodeId: string,
  sstableId: string,
): Promise<StorageEntry[]> => {
  const data = await fetchJson<{ entries: any[] }>(
    `/nodes/${encodeURIComponent(nodeId)}/sstables/${encodeURIComponent(sstableId)}`,
  );
  return (data.entries || []).map(e => ({
    key: e.key,
    value: e.value,
    vectorClock: e.vector_clock ?? {},
  }));
};
export const checkHotPartitions = async (): Promise<void> => {
  await fetchJson<{ status: string }>('/cluster/actions/check_hot_partitions', {
    method: 'POST',
  });
};

export const resetMetrics = async (): Promise<void> => {
  await fetchJson<{ status: string }>('/cluster/actions/reset_metrics', {
    method: 'POST',
  });
};

export const markHotKey = async (
  key: string,
  buckets: number,
  migrate = false,
): Promise<void> => {
  const params = new URLSearchParams({ key, buckets: String(buckets) });
  if (migrate) params.append('migrate', 'true');
  await fetchJson<{ status: string }>(
    `/cluster/actions/mark_hot_key?${params.toString()}`,
    { method: 'POST' },
  );
};

export const splitPartition = async (
  pid: number,
  splitKey?: string,
): Promise<void> => {
  const params = new URLSearchParams({ pid: String(pid) });
  if (splitKey) params.append('split_key', splitKey);
  await fetchJson<{ status: string }>(
    `/cluster/actions/split_partition?${params.toString()}`,
    { method: 'POST' },
  );
};

export const mergePartitions = async (
  pid1: number,
  pid2: number,
): Promise<void> => {
  const params = new URLSearchParams({
    pid1: String(pid1),
    pid2: String(pid2),
  });
  await fetchJson<{ status: string }>(
    `/cluster/actions/merge_partitions?${params.toString()}`,
    { method: 'POST' },
  );
};

export const rebalance = async (): Promise<void> => {
  await fetchJson<{ status: string }>('/cluster/actions/rebalance', {
    method: 'POST',
  });
};

export const getTransactions = async (): Promise<TransactionInfo[]> => {
  const data = await fetchJson<{ transactions: { node: string; tx_ids: string[] }[] }>(
    '/cluster/transactions',
  );
  const results: TransactionInfo[] = [];
  for (const entry of data.transactions || []) {
    for (const id of entry.tx_ids || []) {
      results.push({ node: entry.node, txId: id });
    }
  }
  return results;
};

export const abortTransaction = async (
  nodeId: string,
  txId: string,
): Promise<void> => {
  await fetchJson<{ status: string }>(
    `/cluster/transactions/${encodeURIComponent(nodeId)}/${encodeURIComponent(txId)}/abort`,
    { method: 'POST' },
  );
};

export const getClusterEvents = async (
  offset = 0,
  limit?: number,
): Promise<string[]> => {
  const params = new URLSearchParams();
  if (offset) params.append('offset', String(offset));
  if (typeof limit === 'number') params.append('limit', String(limit));
  const qs = params.size ? `?${params.toString()}` : '';
  const data = await fetchJson<{ events: string[] }>(`/cluster/events${qs}`);
  return data.events || [];
};

export const getNodeEvents = async (
  nodeId: string,
  offset = 0,
  limit?: number,
): Promise<string[]> => {
  const params = new URLSearchParams();
  if (offset) params.append('offset', String(offset));
  if (typeof limit === 'number') params.append('limit', String(limit));
  const qs = params.size ? `?${params.toString()}` : '';
  const data = await fetchJson<{ events: string[] }>(
    `/nodes/${encodeURIComponent(nodeId)}/events${qs}`,
  );
  return data.events || [];
};

export interface SqlQueryResult {
  columns: { name: string; type: string }[];
  rows: any[];
}

export const runSqlQuery = async (sql: string): Promise<SqlQueryResult> => {
  const data = await fetchJson<any>('/sql/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  return { columns: data.columns || [], rows: data.rows || [] };
};

export const executeSql = async (sql: string): Promise<void> => {
  await fetchJson('/sql/execute', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
};
