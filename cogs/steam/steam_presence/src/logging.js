'use strict';

const fs = require('fs');
const path = require('path');

// Resolve project root (…/Deadlock-Steam-Bot) from this module location.
const PROJECT_ROOT = path.resolve(__dirname, '..', '..', '..', '..');

const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const LOG_THRESHOLD = Object.prototype.hasOwnProperty.call(LOG_LEVELS, LOG_LEVEL)
  ? LOG_LEVELS[LOG_LEVEL]
  : LOG_LEVELS.info;

const STEAM_LOG_FILE = path.join(PROJECT_ROOT, 'logs', 'steam_bridge.log');
const MAX_LOG_LINES = 10000;
let steamLogLineCount = 0;

function rotateLogFile(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n');
    if (lines.length <= MAX_LOG_LINES) {
      return lines.length;
    }
    const newContent = lines.slice(-MAX_LOG_LINES).join('\n');
    fs.writeFileSync(filePath, newContent, 'utf8');
    return MAX_LOG_LINES;
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      return 0;
    }
    return 0;
  }
}

// Initial rotation check
steamLogLineCount = rotateLogFile(STEAM_LOG_FILE);

function log(level, message, extra = undefined) {
  const lvl = LOG_LEVELS[level];
  if (lvl === undefined || lvl > LOG_THRESHOLD) return;
  const payload = { time: new Date().toISOString(), level, msg: message };
  if (extra && typeof extra === 'object') {
    for (const [key, value] of Object.entries(extra)) {
      if (value === undefined) continue;
      payload[key] = value;
    }
  }
  // Try console first (ignore EPIPE)
  try {
    console.log(JSON.stringify(payload));
  } catch (_) {
    /* ignore broken pipe */
  }

  // Also write to file
  try {
    fs.appendFileSync(STEAM_LOG_FILE, JSON.stringify(payload) + '\n', 'utf8');
    steamLogLineCount++;
    if (steamLogLineCount > MAX_LOG_LINES + 500) {
      steamLogLineCount = rotateLogFile(STEAM_LOG_FILE);
    }
  } catch (_) {
    /* ignore */
  }
}

function convertKeysToCamelCase(obj) {
  if (Array.isArray(obj)) {
    return obj.map((v) => convertKeysToCamelCase(v));
  }
  if (obj !== null && obj.constructor === Object) {
    return Object.keys(obj).reduce((result, key) => {
      const camelCaseKey = key.replace(/_([a-z])/g, (g) => g[1].toUpperCase());
      result[camelCaseKey] = convertKeysToCamelCase(obj[key]);
      return result;
    }, {});
  }
  return obj;
}

module.exports = {
  log,
  rotateLogFile,
  convertKeysToCamelCase,
  LOG_LEVELS,
  LOG_LEVEL,
  LOG_THRESHOLD,
  STEAM_LOG_FILE,
  MAX_LOG_LINES,
  PROJECT_ROOT,
};
