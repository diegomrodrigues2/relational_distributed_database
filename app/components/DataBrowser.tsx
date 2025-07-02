import React, { useState, useEffect, useCallback } from 'react';
import * as mockDatabaseService from '../services/mockDatabaseService';
import { UserRecord } from '../types';
import DataTable from './databrowser/DataTable';
import DataEditorModal from './databrowser/DataEditorModal';
import Button from './common/Button';

const DataBrowser: React.FC = () => {
  const [records, setRecords] = useState<UserRecord[]>([]);
  const [filteredRecords, setFilteredRecords] = useState<UserRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingRecord, setEditingRecord] = useState<UserRecord | null>(null);
  const [searchTerm, setSearchTerm] = useState('');

  const fetchData = useCallback(async () => {
    setIsLoading(true);
    try {
      const recordsData = await mockDatabaseService.getUserRecords();
      setRecords(recordsData);
      setFilteredRecords(recordsData);
    } catch (error) {
      console.error("Failed to fetch user records:", error);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    const lowercasedFilter = searchTerm.toLowerCase();
    const filteredData = records.filter(item => {
      return (
        item.partitionKey.toLowerCase().includes(lowercasedFilter) ||
        item.clusteringKey.toLowerCase().includes(lowercasedFilter) ||
        item.value.toLowerCase().includes(lowercasedFilter)
      );
    });
    setFilteredRecords(filteredData);
  }, [searchTerm, records]);


  const handleOpenCreateModal = () => {
    setEditingRecord(null);
    setIsModalOpen(true);
  };
  
  const handleOpenEditModal = (record: UserRecord) => {
    setEditingRecord(record);
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
    setEditingRecord(null);
  };

  const handleSaveRecord = async (record: UserRecord) => {
    await mockDatabaseService.saveUserRecord(record);
    handleCloseModal();
    await fetchData(); // Refresh data
  };

  const handleDeleteRecord = async (partitionKey: string, clusteringKey: string) => {
    if (window.confirm(`Are you sure you want to delete record with key ${partitionKey} | ${clusteringKey}?`)) {
        await mockDatabaseService.deleteUserRecord(partitionKey, clusteringKey);
        await fetchData();
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Data Browser</h1>
        <p className="text-green-300 mt-1">Directly query, create, edit, and delete records.</p>
      </div>

      <div className="flex justify-between items-center">
        <input
          type="text"
          placeholder="Search by key or value..."
          value={searchTerm}
          onChange={e => setSearchTerm(e.target.value)}
          className="w-full max-w-sm bg-[#10180f] border border-green-700/50 rounded-md px-3 py-2 text-white placeholder-green-400 focus:outline-none focus:ring-2 focus:ring-green-500"
        />
        <Button onClick={handleOpenCreateModal}>Create New Record</Button>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-500"></div>
        </div>
      ) : (
        <DataTable records={filteredRecords} onEdit={handleOpenEditModal} onDelete={handleDeleteRecord} />
      )}
      
      <DataEditorModal 
        isOpen={isModalOpen}
        onClose={handleCloseModal}
        onSave={handleSaveRecord}
        record={editingRecord}
      />
    </div>
  );
};

export default DataBrowser;