import React from 'react';
import { Icon } from '../lib/icons';

export default function TopBar({
  mode, onMode, theme, onToggleTheme, setup, onOpenSetup,
  onToggleRail, onToggleDock, railHidden, dockHidden, onToggleFocus,
}) {
  const home = mode === 'landing';
  const ready = setup?.initialized;
  const setupLabel = !setup ? 'Checking workspace' : ready ? 'Workspace ready' : 'Workspace setup';
  return (
    <header className={`topbar ${home ? 'is-home' : ''}`}>
      <button className="brand" onClick={() => onMode('landing')} aria-label="Data Model Studio home" title="Home">
        <span className="brand-mark"><Icon name="model" /></span>
        <span className="brand-name">Data Model Studio<span>by Databricks</span></span>
      </button>
      <span className="topbar-divider" aria-hidden="true" />
      <nav className="mode-tabs" aria-label="Workspace">
        <button className="mode-tab" aria-current={mode === 'explore' ? 'page' : undefined} onClick={() => onMode('explore')}>
          <Icon name="database" className="icon-sm" /><span className="mode-label">Explore</span>
        </button>
        <button className="mode-tab" aria-current={mode === 'design' ? 'page' : undefined} onClick={() => onMode('design')}>
          <Icon name="model" className="icon-sm" /><span className="mode-label">Design</span>
        </button>
      </nav>
      <div className="topbar-spacer" />
      <div className="topbar-actions">
        {!home && (
          <div className="panel-toggles">
            <button className="btn btn-ghost btn-icon" title={railHidden ? 'Show sources' : 'Hide sources'} aria-label={railHidden ? 'Show sources' : 'Hide sources'} aria-controls="sources-panel" aria-pressed={!railHidden} onClick={onToggleRail}>
              <Icon name="panelLeft" />
            </button>
            <button className="btn btn-ghost btn-icon" title={dockHidden ? 'Show properties and tools' : 'Hide properties and tools'} aria-label={dockHidden ? 'Show properties and tools' : 'Hide properties and tools'} aria-controls="tools-panel" aria-pressed={!dockHidden} onClick={onToggleDock}>
              <Icon name="panelRight" />
            </button>
            <button className="btn btn-ghost btn-icon focus-toggle" title="Focus canvas (F)" aria-label="Toggle canvas focus mode" aria-pressed={railHidden && dockHidden} onClick={onToggleFocus}>
              <Icon name={railHidden && dockHidden ? 'shrink' : 'expand'} />
            </button>
          </div>
        )}
        <button className={`setup-pill ${!setup ? 'checking' : ready ? 'ok' : ''}`} onClick={onOpenSetup} title={setupLabel} aria-label={setupLabel}>
          <span className="dot" /><span className="setup-label">{!setup ? 'Connecting…' : ready ? 'Workspace ready' : 'Workspace setup'}</span>
        </button>
        <button className="btn btn-ghost btn-icon" onClick={onToggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`} aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
          <Icon name={theme === 'dark' ? 'sun' : 'moon'} />
        </button>
      </div>
    </header>
  );
}
