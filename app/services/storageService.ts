import { Node, ClusterConfig, HotspotInfo, ReplicationStatus, WALEntry, StorageEntry, SSTableInfo, UserRecord } from '../types';
import * as api from './api';
import * as db from './databaseService';

export const getClusterConfig = (): Promise<ClusterConfig> => api.getClusterConfig();
export const getHotspots = (): Promise<HotspotInfo> => api.getHotspots();
export const getReplicationStatus = (): Promise<ReplicationStatus[]> => api.getReplicationStatus();

export const getWalEntries = (
  nodeId: string,
  offset = 0,
  limit = 50,
): Promise<WALEntry[]> => api.getWalEntries(nodeId, offset, limit);
export const getMemtableEntries = (
  nodeId: string,
  offset = 0,
  limit = 50,
): Promise<StorageEntry[]> => api.getMemtableEntries(nodeId, offset, limit);
export const getSstables = (nodeId: string): Promise<SSTableInfo[]> => api.getSstables(nodeId);
export const getSstableEntries = (nodeId: string, sstableId: string): Promise<StorageEntry[]> => api.getSstableEntries(nodeId, sstableId);

export const getUserRecords = (
  offset = 0,
  limit = 50,
  query = '',
): Promise<UserRecord[]> => db.getUserRecords(offset, limit, query);
export const saveUserRecord = (record: UserRecord): Promise<UserRecord> => db.saveUserRecord(record);
export const deleteUserRecord = (partitionKey: string, clusteringKey: string): Promise<void> => db.deleteUserRecord(partitionKey, clusteringKey);

export const addNode = async (): Promise<Node> => {
  const nodeId = await api.addNode();
  const nodes = await api.getNodes();
  return nodes.find(n => n.id === nodeId)!;
};
export const removeNode = (nodeId: string): Promise<string> => api.removeNode(nodeId);
export const stopNode = (nodeId: string): Promise<Node> => api.stopNode(nodeId);
export const startNode = (nodeId: string): Promise<Node> => api.startNode(nodeId);
