import React, { createContext, useCallback, useContext, useEffect, useId, useRef, useState } from 'react';
import { Icon } from '../lib/icons';

const ToastCtx = createContext(null);
export const useToast = () => useContext(ToastCtx);

export function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([]);
  const toast = useCallback((message, type = 'info', ttl = 4500) => {
    const id = Math.random().toString(36).slice(2);
    setToasts((items) => [...items.slice(-3), { id, message, type }]);
    setTimeout(() => setToasts((items) => items.filter((item) => item.id !== id)), ttl);
  }, []);
  return (
    <ToastCtx.Provider value={toast}>
      {children}
      <div className="toast-wrap" aria-live="polite" aria-relevant="additions">
        {toasts.map((item) => (
          <div key={item.id} className={`toast ${item.type === 'error' ? 'err' : ''}`} role="status">
            <Icon name={item.type === 'error' ? 'alert' : 'check'} />
            <span>{item.message}</span>
            <button className="toast-dismiss" aria-label="Dismiss notification" onClick={() => setToasts((items) => items.filter((t) => t.id !== item.id))}><Icon name="x" className="icon-sm" /></button>
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}

export function Spinner({ lg }) {
  return <span className={`spinner ${lg ? 'lg' : ''}`} role="status" aria-label="Loading" />;
}

export function Empty({ icon = 'boxes', title, children }) {
  return (
    <div className="empty">
      <div className="empty-inner">
        <span className="empty-icon"><Icon name={icon} /></span>
        {title && <h3>{title}</h3>}
        {children && <p>{children}</p>}
      </div>
    </div>
  );
}

export function InlineError({ title = 'Something went wrong', message, onRetry }) {
  return (
    <div className="inline-error" role="alert">
      <Icon name="alert" />
      <div><strong>{title}</strong>{message && <p>{message}</p>}{onRetry && <button className="text-button" onClick={onRetry}><Icon name="refresh" className="icon-sm" /> Try again</button>}</div>
    </div>
  );
}

export function Modal({ title, description, icon = 'settings', onClose, children }) {
  const ref = useRef(null);
  const titleId = useId();
  const descriptionId = useId();
  useEffect(() => {
    const previous = document.activeElement;
    const dialog = ref.current;
    dialog.showModal();
    return () => { dialog.close(); previous?.focus?.(); };
  }, []);
  return (
    <dialog ref={ref} className="modal" aria-labelledby={titleId} aria-describedby={description ? descriptionId : undefined}
      onCancel={(event) => { event.preventDefault(); onClose(); }}
      onClick={(event) => { if (event.target === ref.current) onClose(); }}>
      <div className="modal-content">
        <div className="modal-heading">
          <span className="modal-icon"><Icon name={icon} /></span>
          <h2 id={titleId}>{title}</h2>
          <button className="btn btn-ghost btn-icon" aria-label="Close dialog" onClick={onClose} autoFocus><Icon name="x" /></button>
        </div>
        {description && <p className="modal-description" id={descriptionId}>{description}</p>}
        {children}
      </div>
    </dialog>
  );
}

export function Tabs({ tabs, value, onChange, label, id, className = 'dock-tabs', tabClassName = 'dock-tab' }) {
  const onKeyDown = (event) => {
    const index = tabs.findIndex((tab) => tab.id === value);
    let next;
    if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
    else if (event.key === 'ArrowLeft') next = (index - 1 + tabs.length) % tabs.length;
    else if (event.key === 'Home') next = 0;
    else if (event.key === 'End') next = tabs.length - 1;
    else return;
    event.preventDefault();
    onChange(tabs[next].id);
    document.getElementById(`${id}-tab-${tabs[next].id}`)?.focus();
  };
  return (
    <div className={className} role="tablist" aria-label={label} onKeyDown={onKeyDown}>
      {tabs.map((tab) => <button key={tab.id} className={tabClassName} role="tab" id={`${id}-tab-${tab.id}`}
        aria-selected={value === tab.id} aria-controls={`${id}-panel-${tab.id}`} tabIndex={value === tab.id ? 0 : -1}
        onClick={() => onChange(tab.id)}>{tab.icon && <Icon name={tab.icon} />}{tab.label}</button>)}
    </div>
  );
}

export function PanelDivider({ label, value, min, max, direction = 1, onChange, hidden }) {
  const drag = useRef(null);
  const finish = () => {
    if (!drag.current) return;
    document.body.style.cursor = drag.current.cursor;
    document.body.style.userSelect = drag.current.userSelect;
    drag.current = null;
  };
  useEffect(() => () => finish(), []);
  const resize = (next) => onChange(Math.round(Math.min(max, Math.max(min, next))));
  return (
    <div className={`panel-divider ${direction === -1 ? 'end' : 'start'}`} role="separator" tabIndex={hidden ? -1 : 0} hidden={hidden}
      aria-label={label} aria-orientation="vertical" aria-controls={direction === 1 ? 'sources-panel' : 'tools-panel'} aria-valuenow={value} aria-valuemin={min} aria-valuemax={max}
      onKeyDown={(event) => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault();
        resize(event.key === 'Home' ? min : event.key === 'End' ? max : value + (event.key === 'ArrowRight' ? 12 : -12) * direction);
      }}
      onPointerDown={(event) => {
        if (event.button !== 0) return;
        event.preventDefault();
        drag.current = { x: event.clientX, value, cursor: document.body.style.cursor, userSelect: document.body.style.userSelect };
        event.currentTarget.setPointerCapture(event.pointerId);
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
      }}
      onPointerMove={(event) => { if (drag.current) resize(drag.current.value + (event.clientX - drag.current.x) * direction); }}
      onPointerUp={finish} onPointerCancel={finish} onLostPointerCapture={finish} />
  );
}

export async function copyText(text) {
  try { await navigator.clipboard.writeText(text); return true; }
  catch { return false; }
}

export function download(name, content) {
  const blob = new Blob([content], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = name;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
