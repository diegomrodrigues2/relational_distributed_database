import React from 'react';
import Button from '../common/Button';

interface PaginationProps {
  currentPage: number;
  onPageChange: (page: number) => void;
  disablePrev?: boolean;
  disableNext?: boolean;
}

const Pagination: React.FC<PaginationProps> = ({ currentPage, onPageChange, disablePrev, disableNext }) => {
  const goPrev = () => onPageChange(currentPage - 1);
  const goNext = () => onPageChange(currentPage + 1);

  return (
    <div className="flex justify-center space-x-2 mt-4">
      <Button variant="secondary" size="sm" onClick={goPrev} disabled={disablePrev}>Previous</Button>
      <span className="text-green-200 self-center">Page {currentPage}</span>
      <Button variant="secondary" size="sm" onClick={goNext} disabled={disableNext}>Next</Button>
    </div>
  );
};

export default Pagination;
