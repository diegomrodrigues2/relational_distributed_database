import React, { useState } from 'react';
import ConfigurationView from './internals/ConfigurationView';
import HotspotsView from './internals/HotspotsView';
import ReplicationStatusView from './internals/ReplicationStatusView';
import Card from './common/Card';

type InternalsTab = 'config' | 'hotspots' | 'replication';

const TabButton: React.FC<{
    label: string;
    isActive: boolean;
    onClick: () => void;
}> = ({ label, isActive, onClick}) => (
    <button
        onClick={onClick}
        className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
            isActive ? 'bg-green-600 text-white' : 'text-green-200 hover:bg-green-800/60'
        }`}
    >
        {label}
    </button>
);


const ClusterInternals: React.FC = () => {
    const [activeTab, setActiveTab] = useState<InternalsTab>('config');

    const renderContent = () => {
        switch(activeTab) {
            case 'config':
                return <ConfigurationView />;
            case 'hotspots':
                return <HotspotsView />;
            case 'replication':
                return <ReplicationStatusView />;
            default:
                return null;
        }
    };
  
    return (
    <div className="space-y-6">
       <div>
        <h1 className="text-3xl font-bold text-white">Cluster Internals</h1>
        <p className="text-green-300 mt-1">Observe the internal configuration, performance hotspots, and replication state.</p>
      </div>

      <Card className="p-2">
        <div className="flex items-center space-x-2">
            <TabButton label="Configuration" isActive={activeTab === 'config'} onClick={() => setActiveTab('config')} />
            <TabButton label="Hotspots" isActive={activeTab === 'hotspots'} onClick={() => setActiveTab('hotspots')} />
            <TabButton label="Replication Status" isActive={activeTab === 'replication'} onClick={() => setActiveTab('replication')} />
        </div>
      </Card>

      <div>
        {renderContent()}
      </div>

    </div>
  );
};

export default ClusterInternals;