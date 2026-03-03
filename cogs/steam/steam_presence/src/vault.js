'use strict';

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

/**
 * Steam Token Vault — Windows Credential Manager + file fallback
 * Context: { log, STEAM_TOKEN_VAULT_ENABLED, STEAM_TOKEN_VAULT_SCRIPT }
 */
module.exports = (ctx) => {
  const { log, STEAM_TOKEN_VAULT_ENABLED, STEAM_TOKEN_VAULT_SCRIPT } = ctx;
  const FALSE_VALUES = new Set(['0', 'false', 'no', 'off']);

  let vaultPythonRunner = null;
  let vaultPythonProbeDone = false;
  let vaultUnavailableLogged = false;
  let vaultScriptMissingLogged = false;
  let insecureFallbackBlockedLogged = false;

  function _flagEnabled(name, defaultValue = true) {
    const raw = String(process.env[name] || '').trim().toLowerCase();
    if (!raw) return defaultValue;
    return !FALSE_VALUES.has(raw);
  }

  function _fileFallbackAllowed() {
    if (process.platform !== 'win32') return true;
    if (!STEAM_TOKEN_VAULT_ENABLED) return true;
    return _flagEnabled('STEAM_ALLOW_INSECURE_FILE_FALLBACK', false);
  }

  function _logBlockedFileFallback(reason, extra = {}) {
    if (insecureFallbackBlockedLogged) return;
    insecureFallbackBlockedLogged = true;
    log('warn', 'Windows vault mode active, refusing insecure token file fallback. Set STEAM_ALLOW_INSECURE_FILE_FALLBACK=1 to opt in.', {
      reason,
      ...extra,
    });
  }

  function _removeFallbackPath(targetPath) {
    try { fs.rmSync(targetPath, { force: true }); } catch (err) { }
  }

  function _cleanupFallbackFiles(primaryPath, legacyPath = null) {
    _removeFallbackPath(primaryPath);
    if (legacyPath) _removeFallbackPath(legacyPath);
  }

  function _resolveVaultRunner() {
    if (!STEAM_TOKEN_VAULT_ENABLED) return null;
    if (vaultPythonProbeDone) return vaultPythonRunner;
    vaultPythonProbeDone = true;

    const configured = String(process.env.STEAM_VAULT_PYTHON || process.env.PYTHON || '').trim();
    const candidates = [];
    if (configured) candidates.push({ cmd: configured, prefix: [] });
    candidates.push({ cmd: 'python', prefix: [] });
    if (process.platform === 'win32') candidates.push({ cmd: 'py', prefix: ['-3'] });

    for (const candidate of candidates) {
      try {
        const probe = spawnSync(
          candidate.cmd,
          [...candidate.prefix, '--version'],
          { encoding: 'utf8', windowsHide: true, timeout: 4000 }
        );
        if (probe.error && probe.error.code === 'ENOENT') continue;
        if (probe.status === 0 || !probe.error) {
          vaultPythonRunner = candidate;
          break;
        }
      } catch (err) {
        continue;
      }
    }

    if (!vaultPythonRunner && !vaultUnavailableLogged) {
      vaultUnavailableLogged = true;
      if (_fileFallbackAllowed()) {
        log('warn', 'Windows vault enabled, but no Python interpreter found. Falling back to token files.');
      } else {
        _logBlockedFileFallback('python_unavailable');
      }
    }
    return vaultPythonRunner;
  }

  function _runVaultCli(command, tokenType, value = null, savedAtIso = null) {
    if (!STEAM_TOKEN_VAULT_ENABLED || !tokenType) return { ok: false, output: '' };
    if (!fs.existsSync(STEAM_TOKEN_VAULT_SCRIPT)) {
      if (!vaultScriptMissingLogged) {
        vaultScriptMissingLogged = true;
        if (_fileFallbackAllowed()) {
          log('warn', 'Steam vault helper script missing. Falling back to token files.', {
            script: STEAM_TOKEN_VAULT_SCRIPT,
          });
        } else {
          _logBlockedFileFallback('helper_missing', { script: STEAM_TOKEN_VAULT_SCRIPT });
        }
      }
      return { ok: false, output: '' };
    }
    const runner = _resolveVaultRunner();
    if (!runner) return { ok: false, output: '' };

    const args = [...runner.prefix, STEAM_TOKEN_VAULT_SCRIPT, command, '--token', tokenType];
    if (command === 'set') {
      args.push('--stdin');
      if (savedAtIso) args.push('--saved-at', savedAtIso);
    }

    let result = null;
    try {
      const spawnOptions = {
        encoding: 'utf8',
        windowsHide: true,
        timeout: 8000,
      };
      if (command === 'set') spawnOptions.input = value == null ? '' : String(value);
      result = spawnSync(runner.cmd, args, spawnOptions);
    } catch (err) {
      log('warn', 'Failed to execute Steam vault helper', {
        operation: command,
        token_type: tokenType,
        error: err && err.message ? err.message : String(err),
      });
      return { ok: false, output: '' };
    }

    if (result.error) {
      log('warn', 'Steam vault helper execution error', {
        operation: command,
        token_type: tokenType,
        error: result.error && result.error.message ? result.error.message : String(result.error),
      });
      return { ok: false, output: '' };
    }
    if (result.status !== 0) {
      log('warn', 'Steam vault helper returned non-zero exit code', {
        operation: command,
        token_type: tokenType,
        exit_code: result.status,
      });
      return { ok: false, output: '' };
    }
    const output = String(result.stdout || '').replace(/\r?\n$/, '');
    return { ok: true, output };
  }

  function _resolveFallbackPaths(filePath, tokenType = null) {
    if (!tokenType) return { primaryPath: filePath, legacyPath: null };
    const primaryPath = path.join(path.dirname(filePath), '.vault', tokenType === 'machine' ? 'mt.bin' : 'rt.bin');
    const legacyPath = primaryPath === filePath ? null : filePath;
    return { primaryPath, legacyPath };
  }

  function _readFallbackTokenFile(targetPath) {
    try {
      const token = fs.readFileSync(targetPath, 'utf8').trim();
      return token || '';
    } catch (err) {
      if (err && err.code === 'ENOENT') return '';
      log('warn', 'Failed to read token file', { path: targetPath, error: err.message });
      return '';
    }
  }

  function _writeFallbackTokenFile(targetPath, value) {
    const dirPath = path.dirname(targetPath);
    fs.mkdirSync(dirPath, { recursive: true });
    try { fs.chmodSync(dirPath, 0o700); } catch (err) { }
    fs.writeFileSync(targetPath, `${value}\n`, 'utf8');
    try { fs.chmodSync(targetPath, 0o600); } catch (err) { }
  }

  function readToken(filePath, tokenType = null) {
    const { primaryPath, legacyPath } = _resolveFallbackPaths(filePath, tokenType);
    if (tokenType) {
      const vaultRead = _runVaultCli('get', tokenType);
      if (vaultRead.ok) return (vaultRead.output || '').trim();
      if (!_fileFallbackAllowed()) {
        _logBlockedFileFallback('read', { token_type: tokenType });
        return '';
      }
    }

    const token = _readFallbackTokenFile(primaryPath);
    if (token) return token;

    if (legacyPath) {
      const legacyToken = _readFallbackTokenFile(legacyPath);
      if (legacyToken) {
        try {
          _writeFallbackTokenFile(primaryPath, legacyToken);
          fs.rmSync(legacyPath, { force: true });
          log('info', 'Migrated legacy Steam token fallback file', { token_type: tokenType, path: primaryPath });
        } catch (err) {
          log('warn', 'Failed to migrate legacy Steam token fallback file', {
            token_type: tokenType,
            path: primaryPath,
            error: err && err.message ? err.message : String(err),
          });
        }
        return legacyToken;
      }
    }

    return '';
  }

  function writeToken(filePath, value, tokenType = null) {
    const normalized = value ? String(value).trim() : '';
    const { primaryPath, legacyPath } = _resolveFallbackPaths(filePath, tokenType);

    if (tokenType) {
      const vaultResult = normalized
        ? _runVaultCli('set', tokenType, normalized)
        : _runVaultCli('delete', tokenType);
      if (vaultResult.ok && vaultResult.output === 'windows_vault') {
        _cleanupFallbackFiles(primaryPath, legacyPath);
        return 'windows_vault';
      }
      if (vaultResult.ok && vaultResult.output === 'file') {
        if (!_fileFallbackAllowed()) {
          _cleanupFallbackFiles(primaryPath, legacyPath);
          _logBlockedFileFallback('write_cli_file', { token_type: tokenType });
          return 'windows_vault_locked';
        }
        if (legacyPath) _removeFallbackPath(legacyPath);
        return 'file';
      }
      if (vaultResult.ok && vaultResult.output === 'windows_vault_locked') {
        if (!normalized) _cleanupFallbackFiles(primaryPath, legacyPath);
        else _logBlockedFileFallback('write_locked', { token_type: tokenType });
        return 'windows_vault_locked';
      }
      if (!_fileFallbackAllowed()) {
        if (!normalized) _cleanupFallbackFiles(primaryPath, legacyPath);
        else _logBlockedFileFallback('write', { token_type: tokenType });
        return 'windows_vault_locked';
      }
    }

    try {
      if (!normalized) {
        _cleanupFallbackFiles(primaryPath, legacyPath);
        return 'file';
      }
      _writeFallbackTokenFile(primaryPath, normalized);
      if (legacyPath) _removeFallbackPath(legacyPath);
      return 'file';
    } catch (err) {
      log('warn', 'Failed to persist token', { path: primaryPath, error: err.message });
      return 'file';
    }
  }

  return { readToken, writeToken };
};
