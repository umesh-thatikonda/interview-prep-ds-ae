'use client';

import Editor, { OnChange } from '@monaco-editor/react';

interface CodeEditorProps {
  code: string;
  language: 'python' | 'sql';
  onChange: OnChange;
}

export default function CodeEditor({ code, language, onChange }: CodeEditorProps) {
  return (
    <div className="h-full w-full border-t border-b border-primary overflow-hidden">
      <Editor
        height="100%"
        defaultLanguage={language}
        defaultValue={code}
        value={code}
        onChange={onChange}
        theme="vs-dark"
        options={{
          minimap: { enabled: false },
          fontSize: 14,
          lineNumbers: 'on',
          scrollBeyondLastLine: false,
          automaticLayout: true,
          padding: { top: 16, bottom: 16 },
        }}
      />
    </div>
  );
}
