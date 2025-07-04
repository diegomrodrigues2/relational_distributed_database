import React, { useState, useEffect } from 'react';
import { UserRecord } from '../../types';
import Button from '../common/Button';

interface DataEditorModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (record: UserRecord) => void;
  record: UserRecord | null;
}

const DataEditorModal: React.FC<DataEditorModalProps> = ({ isOpen, onClose, onSave, record }) => {
  const [partitionKey, setPartitionKey] = useState('');
  const [clusteringKey, setClusteringKey] = useState('');
  const [value, setValue] = useState('');

  useEffect(() => {
    if (record) {
      setPartitionKey(record.partitionKey);
      setClusteringKey(record.clusteringKey || '');
      setValue(record.value);
    } else {
      setPartitionKey('');
      setClusteringKey('');
      setValue('');
    }
  }, [record, isOpen]);

  if (!isOpen) return null;

  const handleValueChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setValue(e.target.value);
  };

  const handleSave = () => {
    onSave({ partitionKey, clusteringKey: clusteringKey || undefined, value });
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-[#19271c] rounded-lg shadow-xl w-full max-w-2xl border border-green-800/60">
        <div className="p-6 border-b border-green-800/60">
          <h2 className="text-xl font-bold text-white">{record ? 'Edit Record' : 'Create New Record'}</h2>
        </div>
        <div className="p-6 space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label htmlFor="partitionKey" className="block text-sm font-medium text-green-300 mb-1">Partition Key</label>
              <input
                type="text"
                id="partitionKey"
                value={partitionKey}
                onChange={e => setPartitionKey(e.target.value)}
                disabled={!!record}
                className="w-full bg-[#10180f] border border-green-700/50 rounded-md px-3 py-2 text-white placeholder-green-500 focus:outline-none focus:ring-2 focus:ring-green-500 disabled:bg-slate-700"
              />
            </div>
            <div>
              <label htmlFor="clusteringKey" className="block text-sm font-medium text-green-300 mb-1">Clustering Key</label>
              <input
                type="text"
                id="clusteringKey"
                value={clusteringKey}
                onChange={e => setClusteringKey(e.target.value)}
                disabled={!!record}
                className="w-full bg-[#10180f] border border-green-700/50 rounded-md px-3 py-2 text-white placeholder-green-500 focus:outline-none focus:ring-2 focus:ring-green-500 disabled:bg-slate-700"
              />
            </div>
          </div>
          <div>
            <label htmlFor="value" className="block text-sm font-medium text-green-300 mb-1">Value</label>
            <textarea
              id="value"
              rows={10}
              value={value}
              onChange={handleValueChange}
              className="w-full bg-[#10180f] border border-green-700/50 rounded-md px-3 py-2 text-white font-mono text-sm placeholder-green-500 focus:outline-none focus:ring-2 focus:ring-green-500"
            />
          </div>
        </div>
        <div className="p-4 bg-[#141f17] border-t border-green-800/60 flex justify-end space-x-3">
          <Button variant="secondary" onClick={onClose}>Cancel</Button>
          <Button variant="primary" onClick={handleSave} disabled={!partitionKey}>
            {record ? 'Save Changes' : 'Create Record'}
          </Button>
        </div>
      </div>
    </div>
  );
};

export default DataEditorModal;