import React, { useState, useEffect } from 'react';
import { getClusterConfig } from '../../services/mock';
import { ClusterConfig } from '../../types';
import Card from '../common/Card';

const ConfigItem: React.FC<{ label: string; value: string | number | undefined }> = ({ label, value }) => (
    <div className="flex justify-between py-3 px-4 bg-green-900/20 rounded-md items-center">
        <dt className="text-green-300 font-medium">{label}</dt>
        <dd className="font-mono text-green-400">{value ?? 'N/A'}</dd>
    </div>
);

const ConfigSection: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
    <div>
        <h4 className="text-md font-semibold text-green-200 mt-4 mb-2">{title}</h4>
        <div className="space-y-2">
            {children}
        </div>
    </div>
);


const ConfigurationView: React.FC = () => {
    const [config, setConfig] = useState<ClusterConfig | null>(null);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchConfig = async () => {
            try {
                const configData = await getClusterConfig();
                setConfig(configData);
            } catch (error) {
                console.error("Failed to fetch cluster config", error);
            } finally {
                setIsLoading(false);
            }
        };
        fetchConfig();
    }, []);

    if (isLoading) {
        return <div className="text-center p-8">Loading configuration...</div>;
    }

    if (!config) {
        return <div className="text-center p-8 text-red-400">Failed to load cluster configuration.</div>;
    }

    return (
        <Card>
            <div className="p-4 border-b border-green-800/60">
                <h3 className="text-lg font-semibold text-green-100">Cluster Configuration</h3>
            </div>
            <div className="p-6">
                <ConfigSection title="Replication & Quorum">
                    <ConfigItem label="Consistency Mode" value={config.consistencyMode.toUpperCase()} />
                    <ConfigItem label="Replication Factor (N)" value={config.replicationFactor} />
                    <ConfigItem label="Write Quorum (W)" value={config.writeQuorum} />
                    <ConfigItem label="Read Quorum (R)" value={config.readQuorum} />
                    <ConfigItem label="Replication Topology" value={config.topology} />
                </ConfigSection>

                <ConfigSection title="Partitioning">
                    <ConfigItem label="Partition Strategy" value={config.partitionStrategy.charAt(0).toUpperCase() + config.partitionStrategy.slice(1)} />
                    <ConfigItem label="Partitions Per Node (vnodes)" value={config.partitionsPerNode} />
                </ConfigSection>

                <ConfigSection title="Performance & Health">
                     <ConfigItem label="Max Transfer Rate" value={`${config.maxTransferRate ? (config.maxTransferRate / 1000000).toFixed(1) : 'N/A'} MB/s`} />
                     <ConfigItem label="Anti-Entropy Interval" value={`${config.antiEntropyInterval}s`} />
                     <ConfigItem label="Replication Max Batch Size" value={config.maxBatchSize} />
                     <ConfigItem label="Heartbeat Interval" value={`${config.heartbeatInterval}s`} />
                     <ConfigItem label="Heartbeat Timeout" value={`${config.heartbeatTimeout}s`} />
                     <ConfigItem label="Hinted Handoff Interval" value={`${config.hintedHandoffInterval}s`} />
                </ConfigSection>
            </div>
        </Card>
    );
};

export default ConfigurationView;