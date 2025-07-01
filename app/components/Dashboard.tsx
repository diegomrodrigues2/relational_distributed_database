import React, { useState, useEffect } from 'react';
import * as dbService from '../services/api';
import { Node, Partition, MetricPoint, ClusterConfig } from '../types';
import ClusterOverview from './dashboard/ClusterOverview';
import NodeList from './dashboard/NodeList';
import PartitionLoadRing from './dashboard/PartitionRing';
import MetricChart from './dashboard/MetricChart';
import PartitionList from './dashboard/PartitionList';
import TopologyView from './dashboard/TopologyView';

interface DashboardProps {
  onManageNode: (nodeId: string) => void;
}

const Dashboard: React.FC<DashboardProps> = ({ onManageNode }) => {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [partitions, setPartitions] = useState<Partition[]>([]);
  const [config, setConfig] = useState<ClusterConfig | null>(null);
  const [metrics, setMetrics] = useState<{
    latency: MetricPoint[];
    throughput: MetricPoint[];
    replicationLogTotal: MetricPoint[];
    hintsTotal: MetricPoint[];
  } | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      try {
        const [nodesData, metricsData, partitionsData, configData] = await Promise.all([
          dbService.getNodes(),
          dbService.getDashboardTimeSeriesMetrics(),
          dbService.getPartitions(),
          dbService.getClusterConfig(),
        ]);
        setNodes(nodesData);
        setMetrics(metricsData);
        setPartitions(partitionsData);
        setConfig(configData);
      } catch (error) {
        console.error("Failed to fetch dashboard data:", error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, []);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-green-500"></div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Cluster Dashboard</h1>
        <p className="text-green-300 mt-1">Health and performance overview of the distributed database.</p>
      </div>

      <ClusterOverview nodes={nodes} />

      <NodeList nodes={nodes} onManageNode={onManageNode} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <PartitionLoadRing partitions={partitions} />
          {config && <TopologyView nodes={nodes} config={config} />}
      </div>
      
      <PartitionList partitions={partitions} />

      {metrics && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <MetricChart title="P99 Latency" data={metrics.latency} lineColor="#4ade80" unit="ms" />
          <MetricChart title="Throughput" data={metrics.throughput} lineColor="#a3e635" unit=" ops/s" />
          <MetricChart title="Total Replication Log" data={metrics.replicationLogTotal} lineColor="#facc15" unit=" items" />
          <MetricChart title="Queued Hinted Handoffs" data={metrics.hintsTotal} lineColor="#f87171" unit=" hints" />
        </div>
      )}
    </div>
  );
};

export default Dashboard;