import React from 'react';
import Button from '../common/Button';

interface PaginationProps {
  currentPage: number;
  onPageChange: (page: number) => void;
  pageSize?: number;
  onPageSizeChange?: (size: number) => void;
  disablePrev?: boolean;
  disableNext?: boolean;
}

const PAGE_SIZE_OPTIONS = [10, 20, 50, 100, 200];

const Pagination: React.FC<PaginationProps> = ({
  currentPage,
  onPageChange,
  pageSize,
  onPageSizeChange,
  disablePrev,
  disableNext,
}) => {
  const goPrev = () => onPageChange(currentPage - 1);
  const goNext = () => onPageChange(currentPage + 1);

  return (
    <div className="flex justify-center items-center space-x-2 mt-4">
      <Button variant="secondary" size="sm" onClick={goPrev} disabled={disablePrev}>Previous</Button>
      <span className="text-green-200 self-center">Page {currentPage}</span>
      <Button variant="secondary" size="sm" onClick={goNext} disabled={disableNext}>Next</Button>
      {onPageSizeChange && pageSize && (
        <select
          value={pageSize}
          onChange={e => onPageSizeChange(Number(e.target.value))}
          className="ml-4 bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
        >
          {PAGE_SIZE_OPTIONS.map(size => (
            <option key={size} value={size}>
              {size} per page
            </option>
          ))}
        </select>
      )}
    </div>
  );
};

export default Pagination;
