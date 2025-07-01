
import React, { useMemo } from 'react';
import { Node, NodeStatus, ClusterConfig } from '../../types';
import Card from '../common/Card';

interface TopologyViewProps {
  nodes: Node[];
  config: ClusterConfig;
}

const NodeCircle: React.FC<{
    cx: number;
    cy: number;
    node: Node;
}> = ({ cx, cy, node }) => {
    const statusColor = {
        [NodeStatus.LIVE]: '#22c55e', // green-500
        [NodeStatus.SUSPECT]: '#eab308', // yellow-500
        [NodeStatus.DEAD]: '#ef4444', // red-500
    }[node.status];

    return (
        <g className="cursor-pointer group">
             <title>{`Node: ${node.id}\nStatus: ${node.status}\nAddress: ${node.address}`}</title>
            <circle cx={cx} cy={cy} r="15" fill={statusColor} stroke="#10180f" strokeWidth="2" className="transition-all duration-300 group-hover:r-[17px]" />
            <text x={cx} y={cy + 4} textAnchor="middle" fill="white" fontSize="10" fontWeight="bold">
                {node.id.split('_')[1]}
            </text>
        </g>
    );
};


const TopologyView: React.FC<TopologyViewProps> = ({ nodes, config }) => {
    const { positions, edges } = useMemo(() => {
        const positions = new Map<string, { x: number; y: number }>();
        const allNodesCount = nodes.length;
        const radius = 80;
        const center = { x: 100, y: 100 };

        if(allNodesCount === 0) return { positions, edges: [] };

        nodes.forEach((node, i) => {
            const angle = (i / allNodesCount) * 2 * Math.PI - Math.PI / 2; // Start from top
            positions.set(node.id, {
                x: center.x + radius * Math.cos(angle),
                y: center.y + radius * Math.sin(angle),
            });
        });

        const activeNodes = nodes.filter(n => n.status !== NodeStatus.DEAD);
        const activeNodeCount = activeNodes.length;
        const edges: [string, string][] = [];
        
        if (activeNodeCount > 1) {
             switch (config.topology) {
                case 'ring':
                    for (let i = 0; i < activeNodeCount; i++) {
                        edges.push([activeNodes[i].id, activeNodes[(i + 1) % activeNodeCount].id]);
                    }
                    break;
                case 'star':
                    if (activeNodes.length > 0) {
                        const hub = activeNodes[0].id;
                        for (let i = 1; i < activeNodeCount; i++) {
                            edges.push([hub, activeNodes[i].id]);
                        }
                    }
                    break;
                case 'all-to-all':
                    for (let i = 0; i < activeNodeCount; i++) {
                        for (let j = i + 1; j < activeNodeCount; j++) {
                            edges.push([activeNodes[i].id, activeNodes[j].id]);
                        }
                    }
                    break;
                default:
                    // For custom or other topologies, we might need more data.
                    // For now, no edges will be drawn.
                    break;
            }
        }

        return { positions, edges };
    }, [nodes, config.topology]);
    
    if (nodes.length === 0) {
        return (
            <Card className="p-4 flex items-center justify-center h-full min-h-[320px]">
                <p className="text-green-300">No nodes to display in topology.</p>
            </Card>
        );
    }

    return (
        <Card className="p-4 h-full min-h-[320px]">
             <h3 className="text-lg font-semibold text-green-100 mb-2">Cluster Topology ({config.topology})</h3>
             <div className="flex items-center justify-center">
                <svg viewBox="0 0 200 200" className="w-full h-full max-w-xs">
                    <g>
                        {edges.map(([from, to], i) => {
                            const p1 = positions.get(from);
                            const p2 = positions.get(to);
                            if (!p1 || !p2) return null;
                            return <line key={i} x1={p1.x} y1={p1.y} x2={p2.x} y2={p2.y} stroke="#2f603b" strokeWidth="1.5" />;
                        })}
                    </g>
                     <g>
                        {nodes.map(node => {
                            const pos = positions.get(node.id);
                            if (!pos) return null;
                            return <NodeCircle key={node.id} cx={pos.x} cy={pos.y} node={node} />;
                        })}
                    </g>
                </svg>
            </div>
            <div className="flex justify-center space-x-4 mt-4 text-xs text-green-300">
                <div className="flex items-center"><div className="w-3 h-3 rounded-full bg-green-500 mr-2"></div>Live</div>
                <div className="flex items-center"><div className="w-3 h-3 rounded-full bg-yellow-500 mr-2"></div>Suspect</div>
                <div className="flex items-center"><div className="w-3 h-3 rounded-full bg-red-500 mr-2"></div>Dead</div>
            </div>
        </Card>
    );
};

export default TopologyView;
