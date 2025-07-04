import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { WALEntry, StorageEntry, SSTableInfo } from '../../types';
import {
    getWalEntries,
    getMemtableEntries,
    getSstables,
} from '../../services/api';
import Card from '../common/Card';

type StorageTab = 'wal' | 'memtable' | 'sstables';

const TabButton: React.FC<{
    label: string;
    isActive: boolean;
    onClick: () => void;
}> = ({ label, isActive, onClick}) => (
    <button
        onClick={onClick}
        className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
            isActive ? 'bg-green-600 text-white' : 'text-green-200 hover:bg-green-900/40'
        }`}
    >
        {label}
    </button>
);

const LoadingSpinner: React.FC = () => (
    <div className="flex items-center justify-center p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-green-400"></div>
    </div>
);

export const WALView: React.FC<{nodeId: string}> = ({ nodeId }) => {
    const [entries, setEntries] = useState<WALEntry[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(20);
    const [searchTerm, setSearchTerm] = useState('');

    const loadData = useCallback(() => {
        setIsLoading(true);
        getWalEntries(nodeId, (currentPage - 1) * pageSize, pageSize).then(data => {
            setEntries(data);
            setIsLoading(false);
        });
    }, [nodeId, currentPage, pageSize]);

    const filtered = useMemo(
        () =>
            entries.filter(e =>
                e.key.toLowerCase().includes(searchTerm.toLowerCase()) ||
                (e.value || '').toLowerCase().includes(searchTerm.toLowerCase())
            ),
        [entries, searchTerm]
    );

    useEffect(() => {
        setCurrentPage(1);
    }, [nodeId]);

    useEffect(() => {
        loadData();
    }, [loadData]);

    if (isLoading) return <LoadingSpinner />;

    return (
        <div className="overflow-x-auto">
            <div className="flex justify-between items-center mb-2">
                <input
                    type="text"
                    placeholder="Filter..."
                    value={searchTerm}
                    onChange={e => { setSearchTerm(e.target.value); }}
                    className="bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
                />
            </div>
             <table className="w-full text-sm text-left text-green-300">
                <thead className="text-xs text-green-400 uppercase bg-green-900/30">
                    <tr>
                        <th className="px-4 py-2">Type</th>
                        <th className="px-4 py-2">Key</th>
                        <th className="px-4 py-2">Value</th>
                    </tr>
                </thead>
                <tbody>
                    {filtered.map((entry, i) => (
                        <tr key={i} className="border-b border-green-800/50">
                            <td className="px-4 py-2">
                                <span className={`font-semibold ${entry.type === 'PUT' ? 'text-green-400' : 'text-red-400'}`}>{entry.type}</span>
                            </td>
                            <td className="px-4 py-2 font-mono">{entry.key}</td>
                            <td className="px-4 py-2 font-mono max-w-xs truncate">{entry.value || 'N/A'}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
            {filtered.length === 0 && <p className="p-4 text-center text-green-400">WAL is empty.</p>}
            <div className="flex items-center justify-center space-x-2 mt-2">
                <button
                    className="px-2 py-1 bg-green-900/40 rounded disabled:opacity-50"
                    disabled={currentPage === 1}
                    onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                >
                    Prev
                </button>
                <span className="text-sm">Page {currentPage}</span>
                <button
                    className="px-2 py-1 bg-green-900/40 rounded disabled:opacity-50"
                    disabled={entries.length < pageSize}
                    onClick={() => setCurrentPage(p => p + 1)}
                >
                    Next
                </button>
                <select
                    value={pageSize}
                    onChange={e => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}
                    className="ml-4 bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
                >
                    {[10,20,50,100,200].map(size => (
                        <option key={size} value={size}>{size} per page</option>
                    ))}
                </select>
            </div>
        </div>
    );
};

export const MemTableView: React.FC<{nodeId: string}> = ({ nodeId }) => {
    const [entries, setEntries] = useState<StorageEntry[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(20);
    const [searchTerm, setSearchTerm] = useState('');

    const loadData = useCallback(() => {
        setIsLoading(true);
        getMemtableEntries(nodeId, (currentPage - 1) * pageSize, pageSize).then(data => {
            setEntries(data);
            setIsLoading(false);
        });
    }, [nodeId, currentPage, pageSize]);

    const filtered = useMemo(
        () =>
            entries.filter(e =>
                e.key.toLowerCase().includes(searchTerm.toLowerCase()) ||
                e.value.toLowerCase().includes(searchTerm.toLowerCase())
            ),
        [entries, searchTerm]
    );

    useEffect(() => {
        setCurrentPage(1);
    }, [nodeId]);

    useEffect(() => {
        loadData();
    }, [loadData]);

    if (isLoading) return <LoadingSpinner />;

    return (
        <div className="overflow-x-auto">
            <div className="flex justify-between items-center mb-2">
                <input
                    type="text"
                    placeholder="Filter..."
                    value={searchTerm}
                    onChange={e => { setSearchTerm(e.target.value); }}
                    className="bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
                />
            </div>
             <table className="w-full text-sm text-left text-green-300">
                <thead className="text-xs text-green-400 uppercase bg-green-900/30">
                    <tr>
                        <th className="px-4 py-2">Key</th>
                        <th className="px-4 py-2">Value</th>
                    </tr>
                </thead>
                <tbody>
                    {filtered.map((entry, i) => (
                        <tr key={i} className="border-b border-green-800/50">
                            <td className="px-4 py-2 font-mono">{entry.key}</td>
                            <td className="px-4 py-2 font-mono max-w-sm truncate">{entry.value}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
            {filtered.length === 0 && <p className="p-4 text-center text-green-400">MemTable is empty.</p>}
            <div className="flex items-center justify-center space-x-2 mt-2">
                <button
                    className="px-2 py-1 bg-green-900/40 rounded disabled:opacity-50"
                    disabled={currentPage === 1}
                    onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                >
                    Prev
                </button>
                <span className="text-sm">Page {currentPage}</span>
                <button
                    className="px-2 py-1 bg-green-900/40 rounded disabled:opacity-50"
                    disabled={entries.length < pageSize}
                    onClick={() => setCurrentPage(p => p + 1)}
                >
                    Next
                </button>
                <select
                    value={pageSize}
                    onChange={e => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}
                    className="ml-4 bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
                >
                    {[10,20,50,100,200].map(size => (
                        <option key={size} value={size}>{size} per page</option>
                    ))}
                </select>
            </div>
        </div>
    );
};

const SSTablesView: React.FC<{nodeId: string}> = ({ nodeId }) => {
    const [tables, setTables] = useState<SSTableInfo[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        setIsLoading(true);
        getSstables(nodeId).then(data => {
            setTables(data);
            setIsLoading(false);
        });
    }, [nodeId]);

    const tablesByLevel = useMemo(() => {
        return tables.reduce((acc, table) => {
            (acc[table.level] = acc[table.level] || []).push(table);
            return acc;
        }, {} as Record<number, SSTableInfo[]>);
    }, [tables]);

    if (isLoading) return <LoadingSpinner />;

    if (tables.length === 0) {
        return <p className="p-4 text-center text-green-400">No SSTables found on this node.</p>;
    }

    return (
        <div className="space-y-4">
            {Object.entries(tablesByLevel).sort(([a], [b]) => Number(a) - Number(b)).map(([level, tablesInLevel]) => (
                 <details key={level} className="bg-green-900/20 rounded-lg overflow-hidden" open>
                    <summary className="px-4 py-3 cursor-pointer hover:bg-green-900/30 font-semibold text-green-100">
                        Level {level} ({tablesInLevel.length} tables, {tablesInLevel.reduce((sum, t) => sum + t.size, 0)} KB total)
                    </summary>
                    <div className="overflow-x-auto border-t border-green-800/60">
                        <table className="w-full text-sm text-left text-green-300">
                            <thead className="text-xs text-green-400 uppercase bg-green-900/40">
                                <tr>
                                    <th className="px-4 py-2">SSTable ID</th>
                                    <th className="px-4 py-2">Key Range</th>
                                    <th className="px-4 py-2 text-right">Items</th>
                                    <th className="px-4 py-2 text-right">Size (KB)</th>
                                </tr>
                            </thead>
                            <tbody>
                                {tablesInLevel.map((table) => (
                                    <tr key={table.id} className="border-b border-green-800/50 last:border-b-0">
                                        <td className="px-4 py-2 font-mono text-green-200">{table.id}</td>
                                        <td className="px-4 py-2 font-mono text-xs">{`[${table.keyRange[0]}, ${table.keyRange[1]})`}</td>
                                        <td className="px-4 py-2 text-right font-mono">{table.itemCount.toLocaleString()}</td>
                                        <td className="px-4 py-2 text-right font-mono">{table.size}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </details>
            ))}
        </div>
    );
};


interface StorageInspectorProps {
    nodeId: string;
}

const StorageInspector: React.FC<StorageInspectorProps> = ({ nodeId }) => {
    const [activeTab, setActiveTab] = useState<StorageTab>('wal');

    const renderContent = () => {
        switch(activeTab) {
            case 'wal': return <WALView nodeId={nodeId} />;
            case 'memtable': return <MemTableView nodeId={nodeId}/>;
            case 'sstables': return <SSTablesView nodeId={nodeId} />;
            default: return null;
        }
    };
    
    return (
        <Card>
            <div className="p-4 border-b border-green-800/60 flex items-center justify-between">
                <h3 className="text-lg font-semibold text-green-100">Storage Inspector</h3>
                <div className="flex items-center space-x-2 p-1 bg-green-900/30 rounded-lg">
                    <TabButton label="WAL" isActive={activeTab === 'wal'} onClick={() => setActiveTab('wal')} />
                    <TabButton label="MemTable" isActive={activeTab === 'memtable'} onClick={() => setActiveTab('memtable')} />
                    <TabButton label="SSTables" isActive={activeTab === 'sstables'} onClick={() => setActiveTab('sstables')} />
                </div>
            </div>
            <div className="p-4">
                {renderContent()}
            </div>
        </Card>
    );
};

export default StorageInspector;