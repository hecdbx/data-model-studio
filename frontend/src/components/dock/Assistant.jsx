import React, { useEffect, useRef, useState } from 'react';
import { streamChat } from '../../lib/api';
import { Icon } from '../../lib/icons';
import { InlineError, Spinner, useToast } from '../common';

const QUICK = [
  { icon: 'code', label: 'Generate SQL for this model', prompt: 'Generate the Unity Catalog DDL for this model' },
  { icon: 'gitBranch', label: 'Build a Lakeflow pipeline', prompt: 'Generate a Lakeflow pipeline for this model' },
  { icon: 'key', label: 'Find missing keys and relationships', prompt: 'Suggest primary and foreign keys I might be missing' },
  { icon: 'columns', label: 'Add audit columns', prompt: 'Add audit columns (_ingested_at, _source_file) to every bronze table' },
];

function stripModelBlock(text) {
  return text.replace(/\x60\x60\x60json\s*([\s\S]*?)\x60\x60\x60/g, (match, body) => /"(entities|removed_entities|removed_relationships)"/.test(body) ? '' : match).trim();
}

function inline(text) {
  return text.split(/(\x60[^\x60]+\x60|\*\*[^*]+\*\*)/g).map((token, i) => {
    if (token.startsWith('\x60') && token.endsWith('\x60')) return <code key={i} className="mono-inline">{token.slice(1, -1)}</code>;
    if (token.startsWith('**') && token.endsWith('**')) return <strong key={i}>{token.slice(2, -2)}</strong>;
    return token;
  });
}

function Markdown({ text }) {
  return text.split(/\x60\x60\x60/).map((part, i) => {
    if (i % 2) return <pre key={i}><code>{part.replace(/^[a-zA-Z]*\n/, '')}</code></pre>;
    return part.split(/\n{2,}/).filter(Boolean).map((paragraph, j) => {
      if (/^#{1,3} /.test(paragraph)) return <h4 key={`${i}-${j}`}>{inline(paragraph.replace(/^#{1,3} /, ''))}</h4>;
      if (paragraph.split('\n').every((line) => /^[-*] /.test(line))) return <ul key={`${i}-${j}`}>{paragraph.split('\n').map((line, k) => <li key={k}>{inline(line.slice(2))}</li>)}</ul>;
      return <p key={`${i}-${j}`}>{inline(paragraph)}</p>;
    });
  });
}

export default function Assistant({ model, pipelineSource, onApplyModel, active }) {
  const toast = useToast();
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const abortRef = useRef(null);
  const requestRef = useRef(0);
  const logRef = useRef(null);

  useEffect(() => () => { requestRef.current += 1; abortRef.current?.(); }, []);
  useEffect(() => {
    if (active && logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [messages, active]);

  const send = (text) => {
    const question = (text ?? input).trim();
    if (!question || streaming) return;
    const request = ++requestRef.current;
    const history = [...messages.map((message) => ({ role: message.role, content: message.rawText ?? message.content })), { role: 'user', content: question }];
    setMessages((previous) => [...previous, { role: 'user', content: question }, { role: 'assistant', content: '', tools: [], streaming: true }]);
    setInput(''); setStreaming(true);
    const patchLast = (patch) => setMessages((previous) => {
      if (!previous.length) return previous;
      const copy = previous.slice();
      copy[copy.length - 1] = { ...copy[copy.length - 1], ...patch(copy[copy.length - 1]) };
      return copy;
    });
    abortRef.current = streamChat({ history, model, pipeline_source: pipelineSource }, (event, data) => {
      if (request !== requestRef.current) return;
      if (event === 'token') patchLast((last) => ({ content: (last.content || '') + data.text }));
      else if (event === 'tool_call') patchLast((last) => ({ tools: [...(last.tools || []), data.name] }));
      else if (event === 'final') {
        patchLast((last) => ({ rawText: data.text || last.content, content: stripModelBlock(data.text || last.content || ''), model: data.model || null, streaming: false }));
        setStreaming(false);
      } else if (event === 'error') {
        patchLast(() => ({ error: data.message, streaming: false }));
        setStreaming(false);
      } else if (event === 'done') {
        patchLast(() => ({ streaming: false }));
        setStreaming(false);
      }
    });
  };

  const stop = () => {
    requestRef.current += 1; abortRef.current?.(); setStreaming(false);
    setMessages((previous) => previous.map((message, i) => i === previous.length - 1 ? { ...message, streaming: false, stopped: true } : message));
  };

  return (
    <div className="chat">
      <div className="chat-context"><span><Icon name="sparkles" /> Claude Opus 5</span><span>{model.entities.length} tables in context</span></div>
      <div className="chat-log" ref={logRef} role="log" aria-label="Conversation with modeling assistant" aria-live="polite" aria-busy={streaming}>
        {!messages.length && <div className="assistant-welcome">
          <span className="assistant-mark"><Icon name="sparkles" /></span>
          <h2>A thinking partner<br />for your data.</h2>
          <p>Refine a model, connect the missing pieces, or turn your design into code. What are you working on?</p>
          <div className="quick-prompts">{QUICK.map((item) => <button key={item.label} className="quick-prompt" onClick={() => send(item.prompt)}><Icon name={item.icon} /><span>{item.label}</span><Icon name="arrowUpRight" /></button>)}</div>
        </div>}
        {messages.map((message, i) => <div key={i} className={`msg ${message.role}`}>
          <span className="role">{message.role === 'assistant' && <Icon name="sparkles" />}{message.role === 'user' ? 'You' : 'Modeling assistant'}</span>
          {!!message.tools?.length && <div className="tool-chips">{message.tools.map((tool, k) => <span key={k} className="tool-chip"><Icon name="settings" className="icon-sm" />{tool.replace(/_/g, ' ')}</span>)}</div>}
          <div className="bubble">
            {message.content ? <Markdown text={message.content} /> : message.streaming ? <span className="tool-chip"><Spinner /> Working on your model…</span> : null}
            {message.error && <InlineError title="Couldn’t finish this response" message={message.error} />}
            {message.model && <div className="apply-cta"><span><Icon name="check" className="icon-sm" /> Model changes are ready to review.</span><button className="btn btn-sm btn-ink" onClick={() => { onApplyModel(message.model); toast('Model changes applied to the canvas.'); }}>Apply to canvas</button></div>}
          </div>
          {message.role === 'assistant' && !message.streaming && <p className="response-note">{message.stopped ? 'Response stopped. ' : ''}AI generated. Review code and changes before applying.</p>}
        </div>)}
      </div>
      <div className="chat-composer">
        <div className="composer-box">
          <textarea aria-label="Message the modeling assistant" placeholder="Describe a change, or ask a question…" rows={3} value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => { if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); send(); } }} />
          <div className="composer-row">
            <span className="composer-context"><Icon name={pipelineSource ? 'gitBranch' : 'model'} />{pipelineSource ? 'Model + pipeline context' : 'Current model attached'}</span>
            {streaming ? <button className="btn btn-sm" onClick={stop} aria-label="Stop response"><Icon name="x" className="icon-sm" /> Stop</button>
              : <button className="btn btn-ink btn-icon" onClick={() => send()} disabled={!input.trim()} aria-label="Send message" title="Send message (Enter)"><Icon name="arrowUpRight" /></button>}
          </div>
        </div>
        <p className="composer-note">AI can make mistakes. Workspace access uses the app’s permissions.</p>
      </div>
    </div>
  );
}
