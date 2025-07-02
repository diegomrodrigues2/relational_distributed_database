import React, { useState } from 'react';
import Card from '../common/Card';
import Button from '../common/Button';
import { splitPartition, mergePartitions } from '../../services/api';

const PartitionActions: React.FC = () => {
  const [splitPid, setSplitPid] = useState('');
  const [splitKey, setSplitKey] = useState('');
  const [mergeLeft, setMergeLeft] = useState('');
  const [mergeRight, setMergeRight] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const handleSplit = async () => {
    if (!splitPid) return;
    setIsLoading(true);
    try {
      await splitPartition(Number(splitPid), splitKey || undefined);
    } catch (err) {
      console.error('Failed to split partition:', err);
    } finally {
      setIsLoading(false);
    }
  };

  const handleMerge = async () => {
    if (!mergeLeft || !mergeRight) return;
    setIsLoading(true);
    try {
      await mergePartitions(Number(mergeLeft), Number(mergeRight));
    } catch (err) {
      console.error('Failed to merge partitions:', err);
    } finally {
      setIsLoading(false);
    }
  };

  const inputClass =
    'w-full bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-white placeholder-green-500 focus:outline-none focus:ring-2 focus:ring-green-500';

  return (
    <Card className="p-6">
      <h3 className="text-xl font-semibold text-green-50 mb-4">Partition Actions</h3>
      <div className="space-y-4">
        <div className="space-y-2 p-4 bg-green-900/20 rounded-lg">
          <h4 className="font-semibold text-green-100">Split Partition</h4>
          <div className="flex flex-col sm:flex-row items-center space-y-2 sm:space-y-0 sm:space-x-2">
            <input
              type="number"
              placeholder="Partition ID"
              value={splitPid}
              onChange={e => setSplitPid(e.target.value)}
              className={inputClass}
            />
            <input
              type="text"
              placeholder="Split Key (optional)"
              value={splitKey}
              onChange={e => setSplitKey(e.target.value)}
              className={inputClass}
            />
            <Button onClick={handleSplit} disabled={!splitPid || isLoading} size="sm">
              Split
            </Button>
          </div>
        </div>
        <div className="space-y-2 p-4 bg-green-900/20 rounded-lg">
          <h4 className="font-semibold text-green-100">Merge Partitions</h4>
          <div className="flex flex-col sm:flex-row items-center space-y-2 sm:space-y-0 sm:space-x-2">
            <input
              type="number"
              placeholder="Left PID"
              value={mergeLeft}
              onChange={e => setMergeLeft(e.target.value)}
              className={inputClass}
            />
            <input
              type="number"
              placeholder="Right PID"
              value={mergeRight}
              onChange={e => setMergeRight(e.target.value)}
              className={inputClass}
            />
            <Button onClick={handleMerge} disabled={!mergeLeft || !mergeRight || isLoading} size="sm">
              Merge
            </Button>
          </div>
        </div>
      </div>
    </Card>
  );
};

export default PartitionActions;
