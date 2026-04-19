'use client';

import { useState, useEffect, useCallback } from 'react';
import { PGlite } from '@electric-sql/pglite';

export function usePostgres() {
  const [db, setDb] = useState<PGlite | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function initPG() {
      try {
        const pg = new PGlite();
        setDb(pg);
        setIsLoading(false);
      } catch (err) {
        console.error('Failed to load PGLite:', err);
        setError('Failed to initialize PostgreSQL environment.');
        setIsLoading(false);
      }
    }

    initPG();
  }, []);

  const runQuery = useCallback(async (sql: string) => {
    if (!db) return { error: 'Database not ready' };

    try {
      const result = await db.query(sql);
      return { output: result, error: null };
    } catch (err: any) {
      return { output: null, error: err.message };
    }
  }, [db]);

  const setupTable = useCallback(async (setupSql: string) => {
    if (!db) return { error: 'Database not ready' };
    try {
      await db.exec(setupSql);
      return { error: null };
    } catch (err: any) {
      return { error: err.message };
    }
  }, [db]);

  return { runQuery, setupTable, isLoading, error };
}
