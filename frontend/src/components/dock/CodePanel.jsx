import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../../lib/api';
import { Icon } from '../../lib/icons';
import { copyText, download, Empty, InlineError, Modal, Spinner, Tabs, useToast } from '../common';

const TABS = [
  { id: 'ddl', label: 'SQL', lang: 'SQL', ext: 'sql' },
  { id: 'sdp', label: 'Lakeflow', lang: 'Python', ext: 'py' },
  { id: 'migration', label: 'Migration', lang: 'SQL', ext: 'sql' },
];
const KEYWORDS = new Set('CREATE OR REPLACE TABLE IF NOT EXISTS ALTER ADD CONSTRAINT PRIMARY FOREIGN KEY REFERENCES COMMENT USING DELTA SELECT FROM WHERE AS JOIN ON AND TRUE FALSE NULL BIGINT STRING INT DOUBLE DECIMAL TIMESTAMP DATE BOOLEAN import def return with for in spark read readStream format load write'.toUpperCase().split(' '));

function HighlightedLine({ line }) {
  return line.split(/(--.*|#.*|'(?:''|[^'])*'|"[^"]*"|\b[A-Za-z_]\w*\b|\b\d+(?:\.\d+)?\b)/g).map((token, i) => {
    let kind = '';
    if (token.startsWith('--') || token.startsWith('#')) kind = 'comment';
    else if (token.startsWith("'") || token.startsWith('"')) kind = 'string';
    else if (KEYWORDS.has(token.toUpperCase())) kind = 'keyword';
    else if (/^\d/.test(token)) kind = 'number';
    return kind ? <span key={i} className={`syntax-${kind}`}>{token}</span> : token;
  });
}

export default function CodePanel({ model, baseModel, target, active }) {
  const toast = useToast();
  const [tab, setTab] = useState('ddl');
  const [code, setCode] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [applying, setApplying] = useState(false);
  const [saving, setSaving] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [runUrl, setRunUrl] = useState('');
  const request = useRef(0);
  const opts = useMemo(() => ({ catalog: target?.catalog, schema_name: target?.schema }), [target?.catalog, target?.schema]);
  const sig = useMemo(() => JSON.stringify({
    name: model.name, description: model.description,
    entities: model.entities.map(({ position, ...entity }) => entity),
    relationships: model.relationships, pipelines: model.pipelines,
  }), [model]);
  const needsBaseline = tab === 'migration' && !baseModel;

  const generate = useCallback(async () => {
    if (!model.entities.length || (tab === 'migration' && !baseModel)) return;
    const current = ++request.current;
    setLoading(true); setError(''); setCode('');
    try {
      const result = tab === 'ddl' ? await api.codegenDDL(model, opts)
        : tab === 'sdp' ? await api.codegenSDP(model, opts)
          : await api.codegenMigration(baseModel, model, opts);
      if (current === request.current) setCode(result.code || '');
    } catch (failure) { if (current === request.current) setError(failure.message); }
    finally { if (current === request.current) setLoading(false); }
  }, [tab, model, baseModel, opts]);

  useEffect(() => {
    if (!active) return;
    setCode(''); setError(''); setRunUrl('');
    if (!model.entities.length || needsBaseline) { setLoading(false); return; }
    setLoading(true);
    const timer = setTimeout(generate, 180);
    return () => { clearTimeout(timer); request.current += 1; };
    // Position changes don't change the generated code.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, tab, sig, opts, baseModel]);

  const meta = TABS.find((item) => item.id === tab);
  const filename = `${(model.name || 'model').trim().replace(/[^a-zA-Z0-9_.-]+/g, '_') || 'model'}_${tab}.${meta.ext}`;
  const targetName = [target?.catalog, target?.schema].filter(Boolean).join('.') || 'App’s configured schema';
  const ready = !!code && !loading && !error;

  const runJob = async () => {
    if (!ready || applying) return;
    setConfirmOpen(false); setApplying(true);
    try {
      const { run_id, run_page_url } = await api.apply(filename, code, 'sql');
      setRunUrl(run_page_url || '');
      toast('SQL apply job started.');
      for (let i = 0; i < 40; i++) {
        await new Promise((resolve) => setTimeout(resolve, 2500));
        const status = await api.applyRun(run_id);
        if (status.result_state) {
          toast(status.result_state === 'SUCCESS' ? 'SQL applied successfully.' : `Apply job finished: ${status.result_state}.`, status.result_state === 'SUCCESS' ? 'info' : 'error');
          break;
        }
        if (['TERMINATED', 'INTERNAL_ERROR'].includes(status.life_cycle_state)) break;
      }
    } catch (failure) { toast(failure.message, 'error'); }
    finally { setApplying(false); }
  };

  const save = async () => {
    if (!ready || saving) return;
    setSaving(true);
    try { const result = await api.saveArtifact(filename, code); toast(`Saved to ${result.path}`); }
    catch (failure) { toast(failure.message, 'error'); }
    finally { setSaving(false); }
  };

  const copy = async () => {
    if (await copyText(code)) toast('Code copied to clipboard.');
    else toast('Couldn’t access the clipboard. Select the code to copy it.', 'error');
  };

  return (
    <div className="code-panel">
      <Tabs tabs={TABS} value={tab} onChange={setTab} label="Generated code format" id="code-output" className="code-tabs" tabClassName="code-tab" />
      {TABS.map((item) => <div key={item.id} className="code-output-content" id={`code-output-panel-${item.id}`} role="tabpanel" aria-labelledby={`code-output-tab-${item.id}`} hidden={tab !== item.id} tabIndex={0}>
        {tab === item.id && (!model.entities.length
          ? <div className="code-empty"><Empty icon="code" title="Your model becomes your code">Add tables or load a model to generate SQL and Lakeflow pipelines.</Empty></div>
          : needsBaseline
            ? <div className="code-empty"><Empty icon="gitBranch" title="See exactly what changed">Load a model from Unity Catalog, make your edits, then return here to generate a migration.</Empty></div>
            : <>
              <div className="code-filebar"><Icon name="file" /><span className="code-filename" title={filename}>{filename}</span><span className="badge">{meta.lang}</span></div>
              <div className="code-actions">
                <span className="code-target" title={`Target: ${targetName}`}>Target: {targetName}</span>
                <button className="btn btn-ghost btn-icon" onClick={generate} disabled={loading || applying} aria-label="Regenerate code" title="Regenerate code"><Icon name="refresh" /></button>
                <button className="btn btn-ghost btn-icon" onClick={copy} disabled={!ready} aria-label="Copy code" title="Copy code"><Icon name="copy" /></button>
                <button className="btn btn-ghost btn-icon" onClick={() => download(filename, code)} disabled={!ready} aria-label="Download code" title="Download code"><Icon name="download" /></button>
              </div>
              {error && <div className="code-feedback"><InlineError title="Couldn’t generate code" message={error} onRetry={generate} /></div>}
              <div className="code-editor" aria-label={`Generated ${meta.lang}`} aria-busy={loading}>
                {loading ? <div className="loading-line"><Spinner /> Generating {meta.lang}…</div>
                  : <pre className="code-body"><code>{code.split('\n').map((line, i) => <span className="code-line" key={i}><span className="line-number" aria-hidden="true">{i + 1}</span><span><HighlightedLine line={line} /></span></span>)}</code></pre>}
              </div>
              <div className="code-footer">
                <p className="code-footer-note">{tab === 'sdp' ? 'A generated pipeline scaffold. Review transformations before running it.' : 'Review the generated SQL before applying it to your workspace.'}</p>
                {runUrl && <a className="text-button" href={runUrl} target="_blank" rel="noopener noreferrer">View apply job <Icon name="arrowUpRight" className="icon-sm" /></a>}
                {tab === 'sdp'
                  ? <button className="btn btn-primary btn-block" onClick={save} disabled={!ready || saving}>{saving ? <Spinner /> : <Icon name="download" className="icon-sm" />}{saving ? 'Saving…' : 'Save pipeline to volume'}</button>
                  : <button className="btn btn-primary btn-block" onClick={() => setConfirmOpen(true)} disabled={!ready || applying}>{applying ? <Spinner /> : <Icon name="play" className="icon-sm" />}{applying ? 'Applying SQL…' : 'Apply SQL to workspace'}</button>}
              </div>
            </>)}
      </div>)}
      {confirmOpen && <Modal title="Apply SQL to your workspace?" icon="play" onClose={() => setConfirmOpen(false)}
        description="The generated SQL will run against the target below. Check the code and target before continuing.">
        <div className="setup-list"><div className="setup-row"><Icon name="database" /><div><strong>Target schema</strong><small>{targetName}</small></div></div><div className="setup-row"><Icon name="file" /><div><strong>SQL file</strong><small>{filename}</small></div></div></div>
        <div className="modal-actions"><button className="btn" onClick={() => setConfirmOpen(false)}>Keep reviewing</button><button className="btn btn-primary" onClick={runJob} disabled={!ready || applying}><Icon name="play" className="icon-sm" /> Apply SQL</button></div>
      </Modal>}
    </div>
  );
}
