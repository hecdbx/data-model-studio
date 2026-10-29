// Thin API client for the Data Model Studio backend.

async function handle(res) {
  let data;
  const text = await res.text();
  try { data = text ? JSON.parse(text) : {}; } catch { data = { error: text }; }
  if (!res.ok || (data && data.error)) {
    throw new Error((data && data.error) || res.statusText || 'Request failed');
  }
  return data;
}

const get = (url) => fetch(url).then(handle);
const post = (url, body) =>
  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  }).then(handle);

const enc = encodeURIComponent;

export const api = {
  health: () => get('/api/health'),
  bootstrapStatus: () => get('/api/bootstrap/status'),
  provision: () => post('/api/bootstrap/provision'),

  // Feature 2 — Unity Catalog
  catalogs: () => get('/api/catalogs'),
  schemas: (catalog) => get(`/api/catalogs/${enc(catalog)}/schemas`),
  modelFromSchemas: (catalog, schemas) => post('/api/model/from-schemas', { catalog, schemas }),

  // Feature 2 — pipelines
  pipelines: () => get('/api/pipelines'),
  pipeline: (id) => get(`/api/pipelines/${enc(id)}`),
  attachPipeline: (id, entities) => post(`/api/pipelines/${enc(id)}/attach`, { entities }),
  parsePipeline: (source, name, entities) => post('/api/pipelines/parse', { source, name, entities }),

  // Feature 1 — volumes
  volumes: (catalog, schema) => get(`/api/catalogs/${enc(catalog)}/schemas/${enc(schema)}/volumes`),
  volumeFiles: (path) => get(`/api/volume-files?path=${enc(path)}`),
  fileSample: (path, maxRows = 30) => get(`/api/file-sample?path=${enc(path)}&max_rows=${maxRows}`),

  // Codegen
  codegenDDL: (model, opts = {}) => post('/api/codegen/ddl', { model, ...opts }),
  codegenSDP: (model, opts = {}) => post('/api/codegen/sdp', { model, ...opts }),
  codegenMigration: (base, edited, opts = {}) => post('/api/codegen/migration', { base, edited, ...opts }),

  // Agent — model from volume (async job)
  generateFromVolume: (body) => post('/api/agent/generate-from-volume', body),
  agentJob: (id) => get(`/api/agent/job/${enc(id)}`),
  inferKeys: (model) => post('/api/agent/infer-keys', { model }),

  // Apply
  saveArtifact: (name, content) => post('/api/artifacts/save', { name, content }),
  apply: (name, content, mode = 'sql') => post('/api/apply', { name, content, mode }),
  applyRun: (runId) => get(`/api/apply/run/${runId}`),
};

/**
 * Stream the agent chat. Calls onEvent(eventName, data) for each SSE event
 * (token, tool_call, tool_result, final, error). Returns an abort function.
 */
export function streamChat({ history, model, pipeline_source }, onEvent) {
  const ctrl = new AbortController();
  (async () => {
    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ history, model, pipeline_source }),
        signal: ctrl.signal,
      });
      if (!res.ok || !res.body) throw new Error(`Chat failed (${res.status})`);
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        const parts = buf.split('\n\n');
        buf = parts.pop() || '';
        for (const part of parts) {
          let ev = 'message', data = '';
          for (const line of part.split('\n')) {
            if (line.startsWith('event:')) ev = line.slice(6).trim();
            else if (line.startsWith('data:')) data += line.slice(5).trim();
          }
          if (!data) continue;
          let parsed;
          try { parsed = JSON.parse(data); } catch { parsed = { raw: data }; }
          onEvent(ev, parsed);
        }
      }
    } catch (e) {
      if (e.name !== 'AbortError') onEvent('error', { message: e.message });
    } finally {
      if (!ctrl.signal.aborted) onEvent('done', {});
    }
  })();
  return () => ctrl.abort();
}
