import React, { useState, useEffect } from 'react';
import { getReplicationStatus } from '../../services/mock';
import { ReplicationStatus } from '../../types';
import Card from '../common/Card';

const ReplicationStatusView: React.FC = () => {
    const [status, setStatus] = useState<ReplicationStatus[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const data = await getReplicationStatus();
                setStatus(data);
            } catch (error) {
                console.error("Failed to fetch replication status:", error);
            } finally {
                setIsLoading(false);
            }
        };
        fetchData();
    }, []);

    if (isLoading) {
        return <div className="text-center p-8">Loading replication status...</div>;
    }

    if (!status.length) {
        return <div className="text-center p-8 text-red-400">Could not load replication status.</div>;
    }

    return (
        <Card className="overflow-x-auto">
            <div className="p-4 border-b border-green-800/60">
                <h3 className="text-lg font-semibold text-green-100">Per-Node Replication State</h3>
            </div>
            <table className="w-full text-sm text-left text-green-300">
                <thead className="text-xs text-green-400 uppercase bg-green-900/30">
                    <tr>
                        <th scope="col" className="px-6 py-3">Node ID</th>
                        <th scope="col" className="px-6 py-3 text-center">Replication Log</th>
                        <th scope="col" className="px-6 py-3">Last Seen Vector</th>
                        <th scope="col" className="px-6 py-3">Hinted Handoffs</th>
                    </tr>
                </thead>
                <tbody>
                    {status.map(nodeStatus => (
                        <tr key={nodeStatus.nodeId} className="border-b border-green-800/60 hover:bg-green-900/20">
                            <td className="px-6 py-4 font-mono text-green-100">{nodeStatus.nodeId}</td>
                            <td className="px-6 py-4 text-center font-mono text-green-100">{nodeStatus.replicationLogSize}</td>
                            <td className="px-6 py-4 font-mono text-xs">
                                {Object.entries(nodeStatus.lastSeen).length > 0 ? (
                                    <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                                    {Object.entries(nodeStatus.lastSeen).map(([nodeId, seq]) => (
                                        <div key={nodeId}><span className="text-green-500">{nodeId}:</span> {seq}</div>
                                    ))}
                                    </div>
                                ) : (
                                    <span className="text-green-500">N/A</span>
                                )}
                            </td>
                            <td className="px-6 py-4">
                                {Object.entries(nodeStatus.hints).length > 0 ? (
                                     Object.entries(nodeStatus.hints).map(([nodeId, count]) => (
                                        <div key={nodeId} className="text-xs">
                                            <span className="font-semibold text-amber-400">{count}</span>
                                            <span className="text-green-400"> hints for </span> 
                                            <span className="font-semibold text-green-200">{nodeId}</span>
                                        </div>
                                     ))
                                ) : (
                                    <span className="text-xs text-green-500">None</span>
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </Card>
    );
};

export default ReplicationStatusView;