
import { Node, NodeStatus, Partition, UserRecord, ClusterConfig, HotspotInfo, ReplicationStatus, WALEntry, StorageEntry, SSTableInfo } from '../types';
import * as api from './api';

let MOCK_NODES: Node[] = [
  { id: 'node_0', address: '192.168.1.100:9000', status: NodeStatus.LIVE, uptime: '28d 4h 12m', cpuUsage: 25.5, memoryUsage: 60.1, diskUsage: 45.2, dataLoad: 1024, replicationLogSize: 5, hintsCount: 12, isCompacting: true },
  { id: 'node_1', address: '192.168.1.101:9000', status: NodeStatus.LIVE, uptime: '28d 4h 11m', cpuUsage: 30.2, memoryUsage: 55.8, diskUsage: 48.9, dataLoad: 1150, replicationLogSize: 2, hintsCount: 15, isCompacting: false },
  { id: 'node_2', address: '192.168.1.102:9000', status: NodeStatus.LIVE, uptime: '15d 1h 5m', cpuUsage: 28.7, memoryUsage: 62.3, diskUsage: 46.7, dataLoad: 1088, replicationLogSize: 8, hintsCount: 10, isCompacting: false },
  { id: 'node_3', address: '192.168.1.103:9000', status: NodeStatus.SUSPECT, uptime: '2d 8h 30m', cpuUsage: 85.1, memoryUsage: 75.0, diskUsage: 60.1, dataLoad: 1520, replicationLogSize: 15, hintsCount: 0, isCompacting: false },
  { id: 'node_4', address: '192.168.1.104:9000', status: NodeStatus.DEAD, uptime: 'N/A', cpuUsage: 0, memoryUsage: 0, diskUsage: 0, dataLoad: 0, replicationLogSize: 0, hintsCount: 0, isCompacting: false },
];

let MOCK_PARTITIONS: Partition[] = [
    { id: 'p0', primaryNodeId: 'node_0', replicaNodeIds: ['node_1', 'node_2'], keyRange: ['0000', '1fff'], size: 512, itemCount: 150230, operationCount: 2100 },
    { id: 'p1', primaryNodeId: 'node_1', replicaNodeIds: ['node_2', 'node_0'], keyRange: ['2000', '3fff'], size: 480, itemCount: 145880, operationCount: 1800 },
    { id: 'p2', primaryNodeId: 'node_2', replicaNodeIds: ['node_0', 'node_1'], keyRange: ['4000', '5fff'], size: 550, itemCount: 155100, operationCount: 3500 },
    { id: 'p3', primaryNodeId: 'node_0', replicaNodeIds: ['node_1', 'node_2'], keyRange: ['6000', '7fff'], size: 490, itemCount: 148300, operationCount: 3100 },
    { id: 'p4', primaryNodeId: 'node_1', replicaNodeIds: ['node_2', 'node_3'], keyRange: ['8000', '9fff'], size: 750, itemCount: 210500, operationCount: 9800 },
    { id: 'p5', primaryNodeId: 'node_2', replicaNodeIds: ['node_3', 'node_0'], keyRange: ['a000', 'bfff'], size: 780, itemCount: 221000, operationCount: 4200 },
    { id: 'p6', primaryNodeId: 'node_3', replicaNodeIds: ['node_0', 'node_1'], keyRange: ['c000', 'dfff'], size: 810, itemCount: 235100, operationCount: 12500 },
    { id: 'p7', primaryNodeId: 'node_0', replicaNodeIds: ['node_1', 'node_2'], keyRange: ['e000', 'ffff'], size: 505, itemCount: 151500, operationCount: 2800 },
];

const MOCK_CLUSTER_CONFIG: ClusterConfig = {
    consistencyMode: 'lww',
    replicationFactor: 3,
    writeQuorum: 2,
    readQuorum: 2,
    partitionStrategy: 'hash',
    partitionsPerNode: 64,
    topology: 'ring',
    maxTransferRate: 50000000, // 50 MB/s
    antiEntropyInterval: 5.0,
    maxBatchSize: 50,
    heartbeatInterval: 1.0,
    heartbeatTimeout: 3.0,
    hintedHandoffInterval: 1.0,
};



let MOCK_USER_RECORDS: UserRecord[] = [
    { partitionKey: 'user:1001', clusteringKey: 'profile', value: JSON.stringify({ name: 'Alice', email: 'alice@example.com', joined: '2023-01-15' }) },
    { partitionKey: 'user:1002', clusteringKey: 'profile', value: JSON.stringify({ name: 'Bob', email: 'bob@example.com', joined: '2023-02-20' }) },
    { partitionKey: 'product:A-123', clusteringKey: 'inventory', value: JSON.stringify({ location: 'warehouse-1', quantity: 150, last_updated: '2024-05-10' }) },
    { partitionKey: 'product:B-456', clusteringKey: 'inventory', value: JSON.stringify({ location: 'warehouse-2', quantity: 300, last_updated: '2024-05-12' }) },
    { partitionKey: 'order:Z-987', clusteringKey: 'details', value: JSON.stringify({ customer_id: 'user:1001', amount: 99.99, date: '2024-05-11' }) },
];

const MOCK_WAL_ENTRIES: {[nodeId: string]: WALEntry[]} = {
    'node_0': [
        { type: 'PUT', key: 'user:1003|profile', value: '{"name":"Charlie"}', vectorClock: {'node_0': 1891}},
        { type: 'PUT', key: 'session:abc', value: '{"token":"..."}', vectorClock: {'node_0': 1892}},
        { type: 'DELETE', key: 'user:999|temp', vectorClock: {'node_0': 1893}},
    ],
     'node_1': [
        { type: 'PUT', key: 'user:1004|profile', value: '{"name":"Dave"}', vectorClock: {'node_1': 1522}},
    ],
    'node_2': [], 'node_3': [], 'node_4': [],
}

const MOCK_MEMTABLE_ENTRIES: {[nodeId: string]: StorageEntry[]} = {
    'node_0': [
        { key: 'user:1003|profile', value: '{"name":"Charlie"}', vectorClock: {'node_0': 1891}},
        { key: 'session:abc', value: '{"token":"..."}', vectorClock: {'node_0': 1892}},
    ],
    'node_1': [
        { key: 'user:1004|profile', value: '{"name":"Dave"}', vectorClock: {'node_1': 1522}},
    ],
    'node_2': [], 'node_3': [], 'node_4': [],
}

const MOCK_SSTABLES: {[nodeId: string]: SSTableInfo[]} = {
    'node_0': [
        { id: 'sstable_16788899000.txt', level: 0, size: 120, itemCount: 5000, keyRange: ['user:1000', 'user:1999']},
        { id: 'sstable_16788898000.txt', level: 0, size: 115, itemCount: 4800, keyRange: ['product:A-100', 'product:A-999']},
        { id: 'sstable_16788897000.txt', level: 1, size: 500, itemCount: 25000, keyRange: ['order:A-000', 'order:F-999']},
    ],
    'node_1': [
        { id: 'sstable_16788899010.txt', level: 0, size: 125, itemCount: 5100, keyRange: ['user:2000', 'user:2999']},
    ],
    'node_2': [], 'node_3': [], 'node_4': [],
};

const MOCK_SSTABLE_CONTENT: {[nodeId: string]: {[sstableId: string]: StorageEntry[]}} = {
    'node_0': {
        'sstable_16788899000.txt': [
            { key: 'user:1001|profile', value: '{"name":"Alice","email":"alice@example.com"}', vectorClock: {'node_0': 1800}},
            { key: 'user:1002|profile', value: '{"name":"Bob","email":"bob@example.com"}', vectorClock: {'node_1': 1500}},
        ]
    }
}



const mockApi = <T,>(data: T, delay: number = 500): Promise<T> => {
    return new Promise(resolve => {
        setTimeout(() => {
            resolve(JSON.parse(JSON.stringify(data)));
        }, delay);
    });
};

export const getClusterConfig = (): Promise<ClusterConfig> => api.getClusterConfig();

export const getHotspots = (): Promise<HotspotInfo> => api.getHotspots();
export const getReplicationStatus = (): Promise<ReplicationStatus[]> => api.getReplicationStatus();

export const getWalEntries = (nodeId: string): Promise<WALEntry[]> => mockApi(MOCK_WAL_ENTRIES[nodeId] || []);
export const getMemtableEntries = (nodeId: string): Promise<StorageEntry[]> => mockApi(MOCK_MEMTABLE_ENTRIES[nodeId] || []);
export const getSstables = (nodeId: string): Promise<SSTableInfo[]> => mockApi(MOCK_SSTABLES[nodeId] || []);
export const getSstableEntries = (nodeId: string, sstableId: string): Promise<StorageEntry[]> => mockApi(MOCK_SSTABLE_CONTENT[nodeId]?.[sstableId] || []);



export const getUserRecords = (): Promise<UserRecord[]> => mockApi(MOCK_USER_RECORDS);

export const saveUserRecord = (record: UserRecord): Promise<UserRecord> => {
    return new Promise(resolve => {
        setTimeout(() => {
            const index = MOCK_USER_RECORDS.findIndex(r => r.partitionKey === record.partitionKey && r.clusteringKey === record.clusteringKey);
            if (index > -1) {
                MOCK_USER_RECORDS[index] = record;
            } else {
                MOCK_USER_RECORDS.unshift(record);
            }
            resolve(record);
        }, 300);
    });
};

export const deleteUserRecord = (partitionKey: string, clusteringKey: string): Promise<void> => {
    return new Promise(resolve => {
        setTimeout(() => {
            MOCK_USER_RECORDS = MOCK_USER_RECORDS.filter(r => r.partitionKey !== partitionKey || r.clusteringKey !== clusteringKey);
            resolve();
        }, 300);
    });
}

export const addNode = (): Promise<Node> => {
    return new Promise(resolve => {
        setTimeout(() => {
            const newId = `node_${MOCK_NODES.length}`;
            const newNode: Node = {
                id: newId,
                address: `192.168.1.${100 + MOCK_NODES.length}:9000`,
                status: NodeStatus.LIVE,
                uptime: '0d 0h 0m',
                cpuUsage: 0,
                memoryUsage: 0,
                diskUsage: 0,
                dataLoad: 0,
                replicationLogSize: 0,
                hintsCount: 0,
                isCompacting: false,
            };
            MOCK_NODES.push(newNode);
            resolve(JSON.parse(JSON.stringify(newNode)));
        }, 500);
    });
};

export const removeNode = (nodeId: string): Promise<string> => {
    return new Promise(resolve => {
        setTimeout(() => {
            MOCK_NODES = MOCK_NODES.filter(n => n.id !== nodeId);
            resolve(nodeId);
        }, 500);
    });
};

export const stopNode = (nodeId: string): Promise<Node> => {
    return new Promise(resolve => {
        setTimeout(() => {
            const node = MOCK_NODES.find(n => n.id === nodeId);
            if (node) {
                node.status = NodeStatus.DEAD;
                node.uptime = 'N/A';
            }
            resolve(JSON.parse(JSON.stringify(node)));
        }, 300);
    });
};

export const startNode = (nodeId: string): Promise<Node> => {
    return new Promise(resolve => {
        setTimeout(() => {
            const node = MOCK_NODES.find(n => n.id === nodeId);
            if (node) {
                node.status = NodeStatus.LIVE;
                node.uptime = '0d 0h 1m';
            }
            resolve(JSON.parse(JSON.stringify(node)));
        }, 300);
    });
};
