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

  let vaultPythonRunner = null;
  let vaultPythonProbeDone = false;
  let vaultUnavailableLogged = false;
  let vaultScriptMissingLogged = false;

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
      log('warn', 'Windows vault enabled, but no Python interpreter found. Falling back to token files.');
    }
    return vaultPythonRunner;
  }

  function _runVaultCli(command, tokenType, value = null, savedAtIso = null) {
    if (!STEAM_TOKEN_VAULT_ENABLED || !tokenType) return { ok: false, output: '' };
    if (!fs.existsSync(STEAM_TOKEN_VAULT_SCRIPT)) {
      if (!vaultScriptMissingLogged) {
        vaultScriptMissingLogged = true;
        log('warn', 'Steam vault helper script missing. Falling back to token files.', {
          script: STEAM_TOKEN_VAULT_SCRIPT,
        });
      }
      return { ok: false, output: '' };
    }
    const runner = _resolveVaultRunner();
    if (!runner) return { ok: false, output: '' };

    const args = [...runner.prefix, STEAM_TOKEN_VAULT_SCRIPT, command, '--token', tokenType];
    if (command === 'set') {
      args.push('--value', value || '');
      if (savedAtIso) args.push('--saved-at', savedAtIso);
    }

    let result = null;
    try {
      result = spawnSync(runner.cmd, args, { encoding: 'utf8', windowsHide: true, timeout: 8000 });
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

  function readToken(filePath, tokenType = null) {
    if (tokenType) {
      const vaultRead = _runVaultCli('get', tokenType);
      if (vaultRead.ok && vaultRead.output) return vaultRead.output.trim();
    }

    try {
      const token = fs.readFileSync(filePath, 'utf8').trim();
      if (!token) return '';

      // Migrate existing file-based tokens into Windows vault if available.
      if (tokenType && STEAM_TOKEN_VAULT_ENABLED) {
        let savedAtIso = null;
        try {
          savedAtIso = fs.statSync(filePath).mtime.toISOString();
        } catch (err) {
          savedAtIso = null;
        }
        const migrated = _runVaultCli('set', tokenType, token, savedAtIso);
        if (migrated.ok && migrated.output === 'windows_vault') {
          try { fs.rmSync(filePath, { force: true }); } catch (err) { }
          log('info', 'Migrated Steam token from file to Windows vault', { token_type: tokenType });
        }
      }

      return token;
    } catch (err) {
      if (err && err.code === 'ENOENT') return '';
      log('warn', 'Failed to read token file', { path: filePath, error: err.message });
      return '';
    }
  }

  function writeToken(filePath, value, tokenType = null) {
    const normalized = value ? String(value).trim() : '';

    if (tokenType) {
      const vaultResult = normalized
        ? _runVaultCli('set', tokenType, normalized)
        : _runVaultCli('delete', tokenType);
      if (vaultResult.ok && vaultResult.output === 'windows_vault') {
        try { fs.rmSync(filePath, { force: true }); } catch (err) { }
        return 'windows_vault';
      }
      if (vaultResult.ok && vaultResult.output === 'file') {
        return 'file';
      }
    }

    try {
      if (!normalized) {
        fs.rmSync(filePath, { force: true });
        return 'file';
      }
      fs.writeFileSync(filePath, `${normalized}\n`, 'utf8');
      return 'file';
    } catch (err) {
      log('warn', 'Failed to persist token', { path: filePath, error: err.message });
      return 'file';
    }
  }

  return { readToken, writeToken };
};
