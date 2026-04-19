'use client';

import { useState, useEffect } from 'react';
import { Timer as TimerIcon, Play, Pause, RotateCcw } from 'lucide-react';

export default function Timer() {
  const [seconds, setSeconds] = useState(0);
  const [isActive, setIsActive] = useState(false);

  useEffect(() => {
    let interval: any = null;
    if (isActive) {
      interval = setInterval(() => {
        setSeconds((seconds) => seconds + 1);
      }, 1000);
    } else if (!isActive && seconds !== 0) {
      clearInterval(interval);
    }
    return () => clearInterval(interval);
  }, [isActive, seconds]);

  const formatTime = (totalSeconds: number) => {
    const mins = Math.floor(totalSeconds / 60);
    const secs = totalSeconds % 60;
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  const reset = () => {
    setSeconds(0);
    setIsActive(false);
  };

  return (
    <div className="flex items-center gap-4 bg-tertiary px-4 py-2 rounded-lg border border-primary">
      <div className="flex items-center gap-2 text-accent-blue font-mono text-lg">
        <TimerIcon size={18} />
        <span>{formatTime(seconds)}</span>
      </div>
      
      <div className="flex items-center gap-2">
        <button 
          onClick={() => setIsActive(!isActive)}
          className="text-text-primary hover:text-white transition-colors"
        >
          {isActive ? <Pause size={18} /> : <Play size={18} />}
        </button>
        <button 
          onClick={reset}
          className="text-text-primary hover:text-white transition-colors"
        >
          <RotateCcw size={18} />
        </button>
      </div>
    </div>
  );
}
