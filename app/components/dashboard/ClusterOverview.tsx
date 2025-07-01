import React from 'react';
import Card from '../common/Card';
import { Node, NodeStatus } from '../../types';

interface ClusterOverviewProps {
  nodes: Node[];
}

const StatCard: React.FC<{ title: string; value: string | number; colorClass: string }> = ({ title, value, colorClass }) => (
    <Card className="p-4 flex-1">
        <div className="flex items-center">
            <div className={`w-3 h-3 rounded-full mr-3 ${colorClass}`}></div>
            <div>
                <p className="text-sm text-green-300">{title}</p>
                <p className="text-2xl font-bold text-green-50">{value}</p>
            </div>
        </div>
    </Card>
);

const ClusterOverview: React.FC<ClusterOverviewProps> = ({ nodes }) => {
    if (!nodes.length) return null;

    const liveNodes = nodes.filter(n => n.status === NodeStatus.LIVE).length;
    const suspectNodes = nodes.filter(n => n.status === NodeStatus.SUSPECT).length;
    const deadNodes = nodes.filter(n => n.status === NodeStatus.DEAD).length;
    const totalDataLoad = nodes.reduce((acc, node) => acc + node.dataLoad, 0);

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 md:gap-6 mb-8">
      <StatCard title="Live Nodes" value={liveNodes} colorClass="bg-green-500" />
      <StatCard title="Suspect Nodes" value={suspectNodes} colorClass="bg-yellow-500" />
      <StatCard title="Dead Nodes" value={deadNodes} colorClass="bg-red-500" />
      <StatCard title="Total Data Load" value={`${(totalDataLoad / 1024).toFixed(2)} TB`} colorClass="bg-green-500" />
    </div>
  );
};

export default ClusterOverview;