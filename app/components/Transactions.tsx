import React, { useEffect, useState } from 'react';
import Card from './common/Card';
import Button from './common/Button';
import { getTransactions, abortTransaction } from '../services/api';
import { TransactionInfo } from '../types';

const Transactions: React.FC = () => {
  const [transactions, setTransactions] = useState<TransactionInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [aborting, setAborting] = useState<string | null>(null);

  const fetchTx = async () => {
    setLoading(true);
    try {
      const data = await getTransactions();
      setTransactions(data);
    } catch (err) {
      console.error('Failed to fetch transactions', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTx();
  }, []);

  const handleAbort = async (node: string, txId: string) => {
    if (!window.confirm(`Abort transaction ${txId} on ${node}?`)) return;
    setAborting(`${node}:${txId}`);
    try {
      await abortTransaction(node, txId);
      await fetchTx();
    } catch (err) {
      console.error('Failed to abort transaction', err);
    } finally {
      setAborting(null);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Transactions</h1>
        <p className="text-green-300 mt-1">Monitor in-flight transactions and abort if necessary.</p>
      </div>
      <Card className="overflow-x-auto">
        <div className="p-4 border-b border-green-800/60">
          <h3 className="text-lg font-semibold text-green-100">Active Transactions</h3>
        </div>
        {loading ? (
          <div className="p-4 text-center">Loading...</div>
        ) : (
          <table className="w-full text-sm text-left text-green-300">
            <thead className="text-xs text-green-400 uppercase bg-green-900/30">
              <tr>
                <th scope="col" className="px-6 py-3">Node</th>
                <th scope="col" className="px-6 py-3">Transaction ID</th>
                <th scope="col" className="px-6 py-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {transactions.map(tx => (
                <tr key={`${tx.node}_${tx.txId}`} className="border-b border-green-800/60 hover:bg-green-900/20">
                  <td className="px-6 py-4 font-medium text-green-100 whitespace-nowrap">{tx.node}</td>
                  <td className="px-6 py-4">{tx.txId}</td>
                  <td className="px-6 py-4 text-right">
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => handleAbort(tx.node, tx.txId)}
                      disabled={aborting === `${tx.node}:${tx.txId}`}
                    >
                      {aborting === `${tx.node}:${tx.txId}` ? 'Aborting...' : 'Abort'}
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  );
};

export default Transactions;
