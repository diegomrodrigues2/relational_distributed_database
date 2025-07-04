
export enum NodeStatus {
  LIVE = 'LIVE',
  SUSPECT = 'SUSPECT',
  DEAD = 'DEAD',
}

export interface Node {
  id: string;
  address: string;
  status: NodeStatus;
  uptime: string;
  cpuUsage: number;
  memoryUsage: number;
  diskUsage: number;
  dataLoad: number; // in GB
  replicationLogSize: number;
  hintsCount: number;
  isCompacting?: boolean;
}

export interface Partition {
  id:string;
  primaryNodeId: string;
  replicaNodeIds: string[];
  keyRange: [string, string];
  size: number; // in GB
  itemCount: number;
  operationCount: number;
}

export interface MetricPoint {
  time: string;
  value: number;
}

export interface UserRecord {
  partitionKey: string;
  clusteringKey?: string;
  value: string; // free form text
}

export interface ClusterConfig {
    consistencyMode: 'lww' | 'vector' | 'crdt';
    replicationFactor: number;
    writeQuorum: number;
    readQuorum: number;
    partitionStrategy: 'hash' | 'range';
    partitionsPerNode?: number;
    topology: 'all-to-all' | 'ring' | 'star' | 'custom';
    maxTransferRate?: number; // in bytes/sec
    antiEntropyInterval: number; // in seconds
    maxBatchSize: number;
    heartbeatInterval: number; // in seconds
    heartbeatTimeout: number; // in seconds
    hintedHandoffInterval: number; // in seconds
}

export interface HotspotPartition {
    id: string;
    operationCount: number;
    averageOps: number;
}

export interface HotspotKey {
    key: string;
    frequency: number;
}

export interface HotspotInfo {
    hotPartitions: HotspotPartition[];
    hotKeys: HotspotKey[];
}

export interface ReplicationStatus {
    nodeId: string;
    replicationLogSize: number;
    lastSeen: { [nodeId: string]: number };
    hints: { [nodeId: string]: number };
}

export interface WALEntry {
    type: 'PUT' | 'DELETE';
    key: string;
    value?: string;
    vectorClock: { [nodeId: string]: number };
}

export interface StorageEntry {
    key: string;
    value: string;
    vectorClock: { [nodeId: string]: number };
}

export interface SSTableInfo {
    id: string;
    level: number;
    size: number; // in KB
    itemCount: number;
    keyRange: [string, string];
}

export interface TransactionInfo {
    node: string;
    txId: string;
}
