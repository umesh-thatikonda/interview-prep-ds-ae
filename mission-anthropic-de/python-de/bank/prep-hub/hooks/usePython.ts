'use client';

import { useState, useEffect, useCallback } from 'react';

declare global {
  interface Window {
    loadPyodide: any;
  }
}

export function usePython() {
  const [pyodide, setPyodide] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function initPyodide() {
      try {
        if (!window.loadPyodide) {
          const script = document.createElement('script');
          script.src = 'https://cdn.jsdelivr.net/pyodide/v0.25.0/full/pyodide.js';
          script.async = true;
          document.head.appendChild(script);

          await new Promise((resolve) => (script.onload = resolve));
        }

        const instance = await window.loadPyodide({
          indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.25.0/full/',
        });
        
        setPyodide(instance);
        setIsLoading(false);
      } catch (err) {
        console.error('Failed to load Pyodide:', err);
        setError('Failed to initialize Python environment.');
        setIsLoading(false);
      }
    }

    initPyodide();
  }, []);

  const runPython = useCallback(async (code: string) => {
    if (!pyodide) return { error: 'Python environment not ready' };

    try {
      // Redirect stdout to a variable
      await pyodide.runPythonAsync(`
import sys
import io
sys.stdout = io.StringIO()
      `);

      await pyodide.runPythonAsync(code);
      
      const stdout = await pyodide.runPythonAsync('sys.stdout.getvalue()');
      return { output: stdout, error: null };
    } catch (err: any) {
      return { output: null, error: err.message };
    }
  }, [pyodide]);

  return { runPython, isLoading, error };
}
