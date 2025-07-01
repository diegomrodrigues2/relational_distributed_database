import { Node, NodeStatus, Partition, MetricPoint, ClusterConfig } from '../types';

const API_BASE = 'http://localhost:8000';

const fetchJson = async <T>(path: string): Promise<T> => {
  const resp = await fetch(`${API_BASE}${path}`);
  if (!resp.ok) {
    throw new Error(`Request failed: ${resp.status}`);
  }
  return resp.json() as Promise<T>;
};

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
    keyRange: ['', ''],
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
