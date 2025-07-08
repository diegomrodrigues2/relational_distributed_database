import React, { useEffect, useState } from 'react';

interface AlertProps {
  message: string;
  type: 'success' | 'error';
  onClose: () => void;
}

const Alert: React.FC<AlertProps> = ({ message, type, onClose }) => {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    setVisible(true);
    const fade = setTimeout(() => setVisible(false), 2500);
    const timer = setTimeout(onClose, 3000);
    return () => {
      clearTimeout(fade);
      clearTimeout(timer);
    };
  }, [onClose]);

  const base = 'fixed top-4 right-4 px-4 py-2 rounded shadow-lg transition-opacity duration-500';
  const colors = type === 'success' ? 'bg-green-600 text-white' : 'bg-red-600 text-white';

  return (
    <div className={`${base} ${colors} ${visible ? 'opacity-100' : 'opacity-0'}`}>{message}</div>
  );
};

export default Alert;
