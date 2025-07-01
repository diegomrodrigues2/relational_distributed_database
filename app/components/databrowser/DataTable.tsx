import React from 'react';
import { UserRecord } from '../../types';
import Card from '../common/Card';
import Button from '../common/Button';

interface DataTableProps {
  records: UserRecord[];
  onEdit: (record: UserRecord) => void;
  onDelete: (partitionKey: string, clusteringKey: string) => void;
}

const DataTable: React.FC<DataTableProps> = ({ records, onEdit, onDelete }) => {
  return (
    <Card className="overflow-x-auto">
      <table className="w-full text-sm text-left text-green-300">
        <thead className="text-xs text-green-400 uppercase bg-green-900/30">
          <tr>
            <th scope="col" className="px-6 py-3">Partition Key</th>
            <th scope="col" className="px-6 py-3">Clustering Key</th>
            <th scope="col" className="px-6 py-3">Value (JSON)</th>
            <th scope="col" className="px-6 py-3 text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          {records.map((record, index) => (
            <tr key={`${record.partitionKey}-${record.clusteringKey}-${index}`} className="border-b border-green-800/60 hover:bg-green-900/20">
              <td className="px-6 py-4 font-mono text-green-200">{record.partitionKey}</td>
              <td className="px-6 py-4 font-mono text-green-200">{record.clusteringKey}</td>
              <td className="px-6 py-4 font-mono text-lime-400 max-w-md truncate">
                <pre className="whitespace-pre-wrap overflow-x-auto">{record.value}</pre>
              </td>
              <td className="px-6 py-4 text-right space-x-2">
                <Button variant="secondary" size="sm" onClick={() => onEdit(record)}>
                  Edit
                </Button>
                <Button variant="danger" size="sm" onClick={() => onDelete(record.partitionKey, record.clusteringKey)}>
                  Delete
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
       {records.length === 0 && (
        <div className="text-center py-8 text-green-400">
          No records found.
        </div>
      )}
    </Card>
  );
};

export default DataTable;