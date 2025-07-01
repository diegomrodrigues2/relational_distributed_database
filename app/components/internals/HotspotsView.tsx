import React, { useState, useEffect } from 'react';
import { getHotspots } from '../../services/api';
import { HotspotInfo } from '../../types';
import Card from '../common/Card';

const HotspotsView: React.FC = () => {
    const [hotspots, setHotspots] = useState<HotspotInfo | null>(null);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchHotspots = async () => {
            try {
                const data = await getHotspots();
                setHotspots(data);
            } catch (error) {
                console.error("Failed to fetch hotspot data:", error);
            } finally {
                setIsLoading(false);
            }
        };
        fetchHotspots();
    }, []);

    if (isLoading) {
        return <div className="text-center p-8">Loading hotspot data...</div>;
    }

    if (!hotspots) {
        return <div className="text-center p-8 text-red-400">Failed to load hotspot data.</div>;
    }

    return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card className="overflow-x-auto">
                <div className="p-4 border-b border-green-800/60">
                    <h3 className="text-lg font-semibold text-green-100">Hot Partitions</h3>
                    <p className="text-sm text-green-300">Partitions with the highest operational load.</p>
                </div>
                <table className="w-full text-sm text-left text-green-300">
                    <thead className="text-xs text-green-400 uppercase bg-green-900/30">
                        <tr>
                            <th scope="col" className="px-6 py-3">Partition ID</th>
                            <th scope="col" className="px-6 py-3 text-right">Operations</th>
                        </tr>
                    </thead>
                    <tbody>
                        {hotspots.hotPartitions.map(p => (
                            <tr key={p.id} className="border-b border-green-800/60 hover:bg-green-900/20">
                                <td className="px-6 py-4 font-mono text-green-200">{p.id}</td>
                                <td className="px-6 py-4 text-right font-mono">{p.operationCount.toLocaleString()}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </Card>
            <Card className="overflow-x-auto">
                 <div className="p-4 border-b border-green-800/60">
                    <h3 className="text-lg font-semibold text-green-100">Hot Keys</h3>
                    <p className="text-sm text-green-300">Most frequently accessed keys across the cluster.</p>
                </div>
                <table className="w-full text-sm text-left text-green-300">
                    <thead className="text-xs text-green-400 uppercase bg-green-900/30">
                        <tr>
                            <th scope="col" className="px-6 py-3">Key</th>
                            <th scope="col" className="px-6 py-3 text-right">Access Frequency</th>
                        </tr>
                    </thead>
                    <tbody>
                         {hotspots.hotKeys.map(k => (
                            <tr key={k.key} className="border-b border-green-800/60 hover:bg-green-900/20">
                                <td className="px-6 py-4 font-mono text-green-200 max-w-sm truncate">{k.key}</td>
                                <td className="px-6 py-4 text-right font-mono">{k.frequency.toLocaleString()}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </Card>
        </div>
    );
};

export default HotspotsView;