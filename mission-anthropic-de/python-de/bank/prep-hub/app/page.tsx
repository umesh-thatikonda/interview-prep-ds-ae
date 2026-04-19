'use client';

import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import questions from '@/data/questions.json';
import { usePython } from '@/hooks/usePython';
import { usePostgres } from '@/hooks/usePostgres';
import Timer from '@/components/Timer';
import { ChevronLeft, ChevronRight, Play, CheckCircle, AlertCircle, Terminal } from 'lucide-react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const CodeEditor = dynamic(() => import('@/components/CodeEditor'), { ssr: false });

export default function PrepHub() {
  const [currentIdx, setCurrentIdx] = useState(0);
  const [code, setCode] = useState('');
  const [output, setOutput] = useState<any>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [activeTab, setActiveTab] = useState<'statement' | 'output'>('statement');

  const question = questions[currentIdx];
  const { runPython, isLoading: pyLoading } = usePython();
  const { runQuery, setupTable, isLoading: pgLoading } = usePostgres();

  useEffect(() => {
    setCode(question.initialCode);
    setOutput(null);
    setActiveTab('statement');
  }, [currentIdx, question]);

  const handleExecute = async () => {
    setIsExecuting(true);
    setActiveTab('output');
    
    if (question.type === 'python') {
      const res = await runPython(code);
      setOutput(res);
    } else {
      const res = await runQuery(code);
      setOutput(res);
    }
    
    setIsExecuting(false);
  };

  return (
    <div className="flex flex-col h-screen bg-primary text-text-primary overflow-hidden">
      {/* Header */}
      <header className="flex items-center justify-between px-6 py-4 border-b border-primary bg-secondary shadow-md">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-bold tracking-tight text-white flex items-center gap-2">
            <span className="bg-accent-blue w-2 h-8 rounded-full"></span>
            Mission Anthropic
          </h1>
          <div className="h-6 w-px bg-border-primary mx-2"></div>
          <div className="flex items-center gap-3">
            <button 
              disabled={currentIdx === 0}
              onClick={() => setCurrentIdx(i => i - 1)}
              className="p-1 hover:bg-tertiary rounded disabled:opacity-30 transition-all"
            >
              <ChevronLeft />
            </button>
            <span className="font-mono text-sm font-medium min-w-[60px] text-center">
              {currentIdx + 1} / {questions.length}
            </span>
            <button 
              disabled={currentIdx === questions.length - 1}
              onClick={() => setCurrentIdx(i => i + 1)}
              className="p-1 hover:bg-tertiary rounded disabled:opacity-30 transition-all"
            >
              <ChevronRight />
            </button>
          </div>
        </div>

        <div className="flex items-center gap-6">
          <Timer />
          <button 
            onClick={handleExecute}
            disabled={isExecuting || pyLoading || pgLoading}
            className="btn btn-primary flex items-center gap-2 px-6 shadow-lg hover:shadow-accent-blue/20 transition-all"
          >
            {isExecuting ? (
              <span className="animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent"></span>
            ) : (
              <Play size={18} fill="currentColor" />
            )}
            Run Code
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex flex-1 overflow-hidden">
        {/* Left Panel: Question */}
        <div className="w-1/3 flex flex-col border-r border-primary bg-secondary/50">
          <div className="flex border-b border-primary">
            <button 
              onClick={() => setActiveTab('statement')}
              className={cn(
                "px-6 py-3 text-sm font-medium transition-all border-b-2",
                activeTab === 'statement' ? "border-accent-blue text-white bg-tertiary/30" : "border-transparent text-text-secondary hover:text-text-primary"
              )}
            >
              Problem
            </button>
            <button 
              onClick={() => setActiveTab('output')}
              className={cn(
                "px-6 py-3 text-sm font-medium transition-all border-b-2",
                activeTab === 'output' ? "border-accent-blue text-white bg-tertiary/30" : "border-transparent text-text-secondary hover:text-text-primary"
              )}
            >
              Output
            </button>
          </div>

          <div className="flex-1 overflow-y-auto p-8 custom-scrollbar">
            {activeTab === 'statement' ? (
              <div className="space-y-6">
                <div className="flex items-center justify-between">
                  <h2 className="text-2xl font-bold text-white">{question.title}</h2>
                  <span className={cn(
                    "px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider",
                    question.difficulty === 'Easy' ? "bg-accent-green/10 text-accent-green border border-accent-green/20" :
                    question.difficulty === 'Medium' ? "bg-accent-yellow/10 text-accent-yellow border border-accent-yellow/20" :
                    "bg-accent-red/10 text-accent-red border border-accent-red/20"
                  )}>
                    {question.difficulty}
                  </span>
                </div>
                
                <div className="prose prose-invert max-w-none">
                  <p className="text-text-primary leading-relaxed whitespace-pre-wrap text-lg">
                    {question.statement}
                  </p>
                </div>

                {question.testInput && (
                  <div className="space-y-3">
                    <h3 className="text-sm font-bold text-text-secondary uppercase tracking-widest flex items-center gap-2">
                      <Terminal size={14} /> Sample Input
                    </h3>
                    <pre className="bg-bg-primary p-4 rounded-lg border border-primary font-mono text-sm overflow-x-auto text-accent-blue/80">
                      {question.testInput}
                    </pre>
                  </div>
                )}
              </div>
            ) : (
              <div className="space-y-6 animate-in fade-in slide-in-from-bottom-2 duration-300">
                <h3 className="text-sm font-bold text-text-secondary uppercase tracking-widest">Execution Results</h3>
                {isExecuting ? (
                  <div className="flex flex-col items-center justify-center py-12 gap-4 text-text-secondary">
                    <span className="animate-spin rounded-full h-8 w-8 border-4 border-accent-blue border-t-transparent"></span>
                    <p className="animate-pulse">Running your solution...</p>
                  </div>
                ) : output ? (
                  <div className="space-y-6">
                    {output.error ? (
                      <div className="bg-accent-red/10 border border-accent-red/20 p-6 rounded-xl flex gap-4">
                        <AlertCircle className="text-accent-red shrink-0" />
                        <div>
                          <h4 className="font-bold text-accent-red mb-1">Execution Error</h4>
                          <pre className="text-sm font-mono whitespace-pre-wrap text-accent-red/90 opacity-80">{output.error}</pre>
                        </div>
                      </div>
                    ) : (
                      <div className="bg-accent-green/10 border border-accent-green/20 p-6 rounded-xl flex gap-4">
                        <CheckCircle className="text-accent-green shrink-0" />
                        <div>
                          <h4 className="font-bold text-accent-green mb-1">Output Captured</h4>
                          <pre className="text-sm font-mono whitespace-pre-wrap text-text-primary bg-bg-primary/50 p-4 rounded-lg border border-primary mt-3">
                            {typeof output.output === 'object' ? JSON.stringify(output.output, null, 2) : output.output || 'No output produced.'}
                          </pre>
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="flex flex-col items-center justify-center py-12 text-text-muted gap-2 border-2 border-dashed border-primary rounded-xl">
                    <Play size={32} className="opacity-20" />
                    <p>Run your code to see results</p>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Right Panel: Editor */}
        <div className="flex-1 flex flex-col bg-bg-primary">
          <div className="flex items-center justify-between px-6 py-3 bg-secondary/80 border-b border-primary">
            <div className="flex items-center gap-3">
              <span className="w-3 h-3 rounded-full bg-accent-red"></span>
              <span className="w-3 h-3 rounded-full bg-accent-yellow"></span>
              <span className="w-3 h-3 rounded-full bg-accent-green"></span>
              <span className="ml-2 text-xs font-mono text-text-secondary uppercase tracking-widest">{question.type}_editor.v1</span>
            </div>
            <div className="text-xs font-mono text-text-muted">
              {pyLoading || pgLoading ? 'Initializing WASM...' : 'Environment Ready'}
            </div>
          </div>
          <CodeEditor 
            code={code} 
            language={question.type as any} 
            onChange={(val) => setCode(val || '')} 
          />
        </div>
      </main>

      <style jsx global>{`
        .custom-scrollbar::-webkit-scrollbar {
          width: 8px;
        }
        .custom-scrollbar::-webkit-scrollbar-track {
          background: transparent;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb {
          background: var(--border-primary);
          border-radius: 10px;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover {
          background: var(--text-muted);
        }
      `}</style>
    </div>
  );
}
