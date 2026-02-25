#!/usr/bin/env node
'use strict';

/**
 * Steam Bridge – Auth + Task Executor + Quick Invites
 * - Verbindet sich als Headless-Steam-Client
 * - Verarbeitet Tasks aus der SQLite-Tabelle `steam_tasks`
 * Erfordert: steam-user, better-sqlite3
 */

// ---------- Imports ----------
const fs = require('fs');
const os = require('os');
const path = require('path');
const SteamUser = require('steam-user');
const { StatusAnzeige } = require('./statusanzeige');
const { DeadlockGcBot } = require('./deadlock_gc_bot');
const { GcBuildSearch, GC_MSG_FIND_HERO_BUILDS_RESPONSE } = require('./gc_build_search');
const { GcProfileCard } = require('./gc_profile_card');
const { BuildCatalogManager } = require('./build_catalog_manager');
const {
  DEADLOCK_GC_PROTOCOL_OVERRIDE_PATH,
  getHelloPayloadOverride,
  getPlaytestOverrides,
  getOverrideInfo: getGcOverrideInfo,
} = require('./deadlock_gc_protocol');
const {
  log,
  rotateLogFile,
  convertKeysToCamelCase,
  MAX_LOG_LINES,
  PROJECT_ROOT,
} = require('./src/logging');

// ---------- SteamID Helper ----------
let SteamID = null;
if (SteamUser && SteamUser.SteamID) {
  SteamID = SteamUser.SteamID;
} else {
  try {
    SteamID = require('steamid');
  } catch (err) {
    throw new Error(`SteamID helper unavailable: ${err && err.message ? err.message : String(err)}`);
  }
}

async function getPersonaName(accountId) {
  if (!client || !accountId) return String(accountId);
  try {
    const steamId = SteamID.fromIndividualAccountID(accountId);
    const steamId64 = steamId.getSteamID64();
    if (client.users && client.users[steamId64]) {
      return client.users[steamId64].player_name || String(accountId);
    }
    if (typeof client.getPersonas === 'function') {
      const personas = await client.getPersonas([steamId]);
      const persona = personas[steamId64];
      if (persona && persona.player_name) return persona.player_name;
    }
  } catch (err) {
    log('warn', 'Failed to get persona name', { accountId, error: err.message });
  }
  return String(accountId);
}

// ---------- App IDs ----------
const DEADLOCK_APP_IDS = [
  Number.parseInt(process.env.DEADLOCK_APPID || '1422450', 10),
  1422450,
  730,
];
const DEADLOCK_APP_ID = DEADLOCK_APP_IDS[0];
function getWorkingAppId() {
  return DEADLOCK_APP_IDS.find(id => id > 0) || 1422450;
}

// ---------- GC Constants ----------
const PROTO_MASK = SteamUser.GCMsgProtoBuf || 0x80000000;
const GC_MSG_CLIENT_HELLO = 4006;
const GC_MSG_CLIENT_HELLO_ALT = 9018;
const GC_MSG_CLIENT_WELCOME = 4004;
const GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD = 9193;
const GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD_RESPONSE = 9194;

// ---------- Playtest Config ----------
const DEFAULT_PLAYTEST_MSG_IDS = [
  { send: 9189, response: 9190, name: 'original' },
  { send: 9000, response: 9001, name: 'alternative_1' },
  { send: 8000, response: 8001, name: 'alternative_2' },
  { send: 7500, response: 7501, name: 'alternative_3' },
  { send: 10000, response: 10001, name: 'alternative_4' },
];
let playtestMsgConfigs = [...DEFAULT_PLAYTEST_MSG_IDS];
const playtestOverrideConfig = getPlaytestOverrides() || null;
let buildPlaytestPayloadOverrideFn = null;
if (playtestOverrideConfig) {
  if (
    playtestOverrideConfig.messageIds &&
    Number.isFinite(playtestOverrideConfig.messageIds.send) &&
    Number.isFinite(playtestOverrideConfig.messageIds.response)
  ) {
    const overrideEntry = {
      send: Number(playtestOverrideConfig.messageIds.send),
      response: Number(playtestOverrideConfig.messageIds.response),
      name: playtestOverrideConfig.name || 'config_override',
      appId: playtestOverrideConfig.appId,
    };
    if (playtestOverrideConfig.exclusive) playtestMsgConfigs = [overrideEntry];
    else playtestMsgConfigs.unshift(overrideEntry);
  }
  if (typeof playtestOverrideConfig.buildPayload === 'function') {
    buildPlaytestPayloadOverrideFn = playtestOverrideConfig.buildPayload;
  }
}
if (!playtestMsgConfigs.length) playtestMsgConfigs = [...DEFAULT_PLAYTEST_MSG_IDS];

// ---------- Steam Config ----------
const GC_CLIENT_HELLO_PROTOCOL_VERSION_RAW = Number.parseInt(process.env.DEADLOCK_GC_PROTOCOL_VERSION || '1', 10);
const GC_CLIENT_HELLO_PROTOCOL_VERSION =
  Number.isFinite(GC_CLIENT_HELLO_PROTOCOL_VERSION_RAW) && GC_CLIENT_HELLO_PROTOCOL_VERSION_RAW > 0
    ? GC_CLIENT_HELLO_PROTOCOL_VERSION_RAW : 1;
const STEAM_API_KEY = ((process.env.STEAM_API_KEY || process.env.STEAM_WEB_API_KEY || '') + '').trim() || null;
if (STEAM_API_KEY) {
  log('info', 'Steam API key is configured', { length: STEAM_API_KEY.length, prefix: STEAM_API_KEY.substring(0, 3) + '...' });
} else {
  log('warn', 'Steam API key is NOT configured');
}
const WEB_API_FRIEND_CACHE_TTL_MS = Math.max(
  15000,
  Number.isFinite(Number(process.env.STEAM_WEBAPI_FRIEND_CACHE_MS)) ? Number(process.env.STEAM_WEBAPI_FRIEND_CACHE_MS) : 60000
);
const WEB_API_HTTP_TIMEOUT_MS = Math.max(
  5000,
  Number.isFinite(Number(process.env.STEAM_WEBAPI_TIMEOUT_MS)) ? Number(process.env.STEAM_WEBAPI_TIMEOUT_MS) : 12000
);

// ---------- Vault Config ----------
const STEAM_TOKEN_VAULT_SCRIPT = path.join(PROJECT_ROOT, 'cogs', 'steam', 'steam_token_vault_cli.py');
const STEAM_TOKEN_VAULT_ENABLED = process.platform === 'win32'
  && !['0', 'false', 'no', 'off'].includes(String(process.env.STEAM_USE_WINDOWS_VAULT || '1').trim().toLowerCase());
const STEAM_VAULT_REFRESH_TOKEN = 'refresh';
const STEAM_VAULT_MACHINE_TOKEN = 'machine';

// ---------- GC Trace ----------
const GC_TRACE_LOG_PATH = path.join(PROJECT_ROOT, 'logs', 'deadlock_gc_messages.log');
let gcTraceLineCount = 0;
function writeDeadlockGcTrace(event, details = {}) {
  try {
    if (gcTraceLineCount === 0) gcTraceLineCount = rotateLogFile(GC_TRACE_LOG_PATH);
    const line = JSON.stringify({ time: new Date().toISOString(), event, ...details }) + os.EOL;
    fs.appendFileSync(GC_TRACE_LOG_PATH, line, 'utf8');
    gcTraceLineCount++;
    if (gcTraceLineCount > MAX_LOG_LINES + 200) gcTraceLineCount = rotateLogFile(GC_TRACE_LOG_PATH);
  } catch (err) {
    console.error('Failed to write Deadlock GC trace', err && err.message ? err.message : err);
  }
}

// ---------- Utilities ----------
function normalizeToBuffer(value) {
  if (!value && value !== 0) return null;
  if (Buffer.isBuffer(value)) return value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    const hex = trimmed.startsWith('0x') ? trimmed.slice(2) : trimmed;
    if (/^[0-9a-fA-F]+$/.test(hex) && hex.length % 2 === 0) return Buffer.from(hex, 'hex');
    return Buffer.from(trimmed, 'utf8');
  }
  if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  if (value && typeof value === 'object' && value.type === 'Buffer' && Array.isArray(value.data)) return Buffer.from(value.data);
  return null;
}

let gcTokenRequestInFlight = false;
function getDeadlockGcTokenCount() {
  if (!client) return 0;
  const tokens = client._gcTokens;
  if (Array.isArray(tokens)) return tokens.length;
  if (tokens && typeof tokens.length === 'number') return tokens.length;
  return 0;
}
async function requestDeadlockGcTokens(reason = 'unspecified') {
  if (!client || typeof client._sendAuthList !== 'function') {
    log('warn', 'Cannot request Deadlock GC tokens - _sendAuthList unavailable', { reason }); return false;
  }
  if (!client.steamID) { log('debug', 'Skipping GC token request - SteamID missing', { reason }); return false; }
  if (gcTokenRequestInFlight) { log('debug', 'GC token request already in flight', { reason }); return false; }
  gcTokenRequestInFlight = true;
  const haveTokens = getDeadlockGcTokenCount();
  try {
    log('info', 'Requesting Deadlock GC tokens', { reason, haveTokens, appId: DEADLOCK_APP_ID });
    writeDeadlockGcTrace('request_gc_tokens', { reason, haveTokens });
    await client._sendAuthList(DEADLOCK_APP_ID);
    const current = getDeadlockGcTokenCount();
    log('debug', 'GC token request finished', { reason, before: haveTokens, after: current });
    writeDeadlockGcTrace('request_gc_tokens_complete', { reason, before: haveTokens, after: current });
    return true;
  } catch (err) {
    log('error', 'Failed to request Deadlock GC tokens', { reason, error: err && err.message ? err.message : String(err) });
    writeDeadlockGcTrace('request_gc_tokens_failed', { reason, error: err && err.message ? err.message : String(err) });
    return false;
  } finally {
    gcTokenRequestInFlight = false;
  }
}

const nowSeconds = () => Math.floor(Date.now() / 1000);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, Number.isFinite(ms) ? ms : 0)));
function toPositiveInt(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') { if (!Number.isFinite(value) || value <= 0) return null; return Math.floor(value); }
  if (typeof value === 'string' && value.trim().length > 0) { const parsed = Number.parseInt(value, 10); if (!Number.isFinite(parsed) || parsed <= 0) return null; return parsed; }
  return null;
}
function normalizeTimeoutMs(value, fallback, minimum) {
  const parsed = toPositiveInt(value);
  const base = parsed !== null ? parsed : fallback;
  const min = toPositiveInt(minimum);
  return Math.max(min !== null ? min : 0, Number.isFinite(base) ? base : fallback);
}
function normalizeAttempts(value, fallback, maximum = 4) {
  const parsed = toPositiveInt(value);
  const base = parsed !== null ? parsed : fallback;
  const max = toPositiveInt(maximum);
  const clampedMax = max !== null ? max : Math.max(1, base);
  return Math.max(1, Math.min(clampedMax, Number.isFinite(base) ? base : 1));
}
function isTimeoutError(err) {
  if (!err) return false;
  return String(err.message ? err.message : err).toLowerCase().includes('timeout');
}

// ---------- Timing Constants ----------
const MIN_GC_READY_TIMEOUT_MS = 5000;
const DEFAULT_GC_READY_TIMEOUT_MS = normalizeTimeoutMs(process.env.DEADLOCK_GC_READY_TIMEOUT_MS, 120000, MIN_GC_READY_TIMEOUT_MS);
const DEFAULT_GC_READY_ATTEMPTS = normalizeAttempts(process.env.DEADLOCK_GC_READY_ATTEMPTS, 3, 5);
const GC_READY_RETRY_DELAY_MS = normalizeTimeoutMs(process.env.DEADLOCK_GC_READY_RETRY_DELAY_MS, 1500, 250);
const MIN_PLAYTEST_INVITE_TIMEOUT_MS = 5000;
const DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS = normalizeTimeoutMs(process.env.DEADLOCK_PLAYTEST_TIMEOUT_MS, 30000, MIN_PLAYTEST_INVITE_TIMEOUT_MS);
const DEFAULT_PLAYTEST_INVITE_ATTEMPTS = normalizeAttempts(process.env.DEADLOCK_PLAYTEST_RETRY_ATTEMPTS, 3, 5);
const PLAYTEST_RETRY_DELAY_MS = normalizeTimeoutMs(process.env.DEADLOCK_PLAYTEST_RETRY_DELAY_MS, 2000, 250);
const INVITE_RESPONSE_MIN_TIMEOUT_MS = MIN_PLAYTEST_INVITE_TIMEOUT_MS;

// ---------- Config ----------
const DATA_DIR = path.resolve(process.env.STEAM_PRESENCE_DATA_DIR || path.join(__dirname, '.steam-data'));
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (err) { if (err && err.code !== 'EEXIST') throw err; }
const REFRESH_TOKEN_PATH = path.join(DATA_DIR, 'refresh.token');
const MACHINE_TOKEN_PATH = path.join(DATA_DIR, 'machine_auth_token.txt');
const ACCOUNT_NAME = process.env.STEAM_BOT_USERNAME || process.env.STEAM_LOGIN || process.env.STEAM_ACCOUNT || '';
const ACCOUNT_PASSWORD = process.env.STEAM_BOT_PASSWORD || process.env.STEAM_PASSWORD || '';
const TASK_POLL_INTERVAL_MS = parseInt(process.env.STEAM_TASK_POLL_MS || '2000', 10);
const RECONNECT_DELAY_MS = parseInt(process.env.STEAM_RECONNECT_DELAY_MS || '5000', 10);
const COMMAND_BOT_KEY = 'steam';
const COMMAND_POLL_INTERVAL_MS = parseInt(process.env.STEAM_COMMAND_POLL_MS || '2000', 10);
const STATE_PUBLISH_INTERVAL_MS = parseInt(process.env.STEAM_STATE_PUBLISH_MS || '15000', 10);
const GC_UNHEALTHY_THRESHOLD_MS = 15 * 60 * 1000; // 15 minutes
const DB_BUSY_TIMEOUT_MS = Math.max(5000, parseInt(process.env.DEADLOCK_DB_BUSY_TIMEOUT_MS || '15000', 10));
const FRIEND_SYNC_INTERVAL_MS = Math.max(60000, parseInt(process.env.STEAM_FRIEND_SYNC_MS || '300000', 10));
const FRIEND_REQUEST_BATCH_SIZE = Math.max(1, parseInt(process.env.STEAM_FRIEND_REQUEST_BATCH || '10', 10));
const FRIEND_REQUEST_RETRY_SECONDS = Math.max(60, parseInt(process.env.STEAM_FRIEND_REQUEST_RETRY_SEC || '900', 10));
const FRIEND_REQUEST_DAILY_CAP = Math.max(0, parseInt(process.env.STEAM_FRIEND_REQUEST_DAILY_CAP || '10', 10));
const STEAM_TASKS_MAX_ROWS = Math.max(1, parseInt(process.env.STEAM_TASKS_MAX_ROWS || '1000', 10));

const gcOverrideInfo = getGcOverrideInfo();
if (playtestOverrideConfig) {
  log('info', 'Deadlock GC override module active', {
    path: gcOverrideInfo.path,
    messageIdSend: playtestOverrideConfig.messageIds?.send,
    messageIdResponse: playtestOverrideConfig.messageIds?.response,
    exclusive: Boolean(playtestOverrideConfig.exclusive),
  });
} else {
  log('debug', 'No Deadlock GC override module detected', { path: gcOverrideInfo.path });
}

// ---------- Vault + Database ----------
const vault = require('./src/vault')({ log, STEAM_TOKEN_VAULT_ENABLED, STEAM_TOKEN_VAULT_SCRIPT });
const { readToken, writeToken } = vault;

const database = require('./src/database')({ log, DB_BUSY_TIMEOUT_MS, STEAM_TASKS_MAX_ROWS });
const { db } = database;
const {
  STALE_TASK_TIMEOUT_S,
  installSteamTaskCapTrigger, pruneSteamTasks,
  selectPendingTaskStmt, markTaskRunningStmt, resetTaskPendingStmt, failStaleTasksStmt, finishTaskStmt,
  selectHeroBuildSourceStmt, selectHeroBuildCloneMetaStmt, updateHeroBuildCloneUploadedStmt,
  selectPendingCommandStmt, markCommandRunningStmt, finalizeCommandStmt,
  upsertStandaloneStateStmt, steamTaskCountsStmt, steamTaskRecentStmt,
  steamLinksForSyncStmt,
  selectFriendCheckCacheStmt, upsertFriendCheckCacheStmt, countFriendRequestsSentSinceStmt,
  upsertPendingFriendRequestStmt, selectFriendRequestBatchStmt,
  markFriendRequestSentStmt, markFriendRequestSkippedStmt, markFriendRequestFailedStmt,
  deleteFriendRequestStmt, clearFriendFlagStmt,
  verifySteamLinkStmt, unverifySteamLinkStmt,
} = database;
installSteamTaskCapTrigger();
pruneSteamTasks('startup');

// ---------- State ----------
const state = {
  tokens: {
    refreshToken: readToken(REFRESH_TOKEN_PATH, STEAM_VAULT_REFRESH_TOKEN),
    machineAuthToken: readToken(MACHINE_TOKEN_PATH, STEAM_VAULT_MACHINE_TOKEN),
  },
  runtimeState: {
    account_name: ACCOUNT_NAME || null,
    logged_on: false,
    logging_in: false,
    steam_id64: null,
    refresh_token_present: false,
    machine_token_present: false,
    guard_required: null,
    last_error: null,
    last_login_attempt_at: null,
    last_login_source: null,
    last_logged_on_at: null,
    last_disconnect_at: null,
    last_disconnect_eresult: null,
    last_guard_submission_at: null,
    deadlock_gc_ready: false,
  },
  gcUnhealthySince: 0,
  loginInProgress: false,
  pendingGuard: null,
  reconnectTimer: null,
  manualLogout: false,
  deadlockAppActive: false,
  deadlockGameRequestedAt: 0,
  deadlockGcReady: false,
  lastGcHelloAttemptAt: 0,
  deadlockGcWaiters: [],
  pendingPlaytestInviteResponses: [],
  webApiFriendCacheIds: null,
  webApiFriendCacheLastLoadedAt: 0,
  webApiFriendCachePromise: null,
  webApiFriendCacheWarned: false,
  friendCheckCache: new Map(),
  friendSyncInProgress: false,
  // Mutable cross-module state (replaces module-level vars)
  playtestIds: { send: playtestMsgConfigs[0].send, response: playtestMsgConfigs[0].response },
  heroBuildPublishWaiter: null,
  lastLoggedGcTokenCount: 0,
};
state.runtimeState.refresh_token_present = Boolean(state.tokens.refreshToken);
state.runtimeState.machine_token_present = Boolean(state.tokens.machineAuthToken);
const runtimeState = state.runtimeState;

// ---------- Steam Client ----------
const client = new SteamUser();
const deadlockGcBot = new DeadlockGcBot({
  client,
  log: (level, msg, extra) => log(level, msg, extra),
  trace: writeDeadlockGcTrace,
  requestTokens: (reason) => requestDeadlockGcTokens(reason || 'deadlock_gc_bot'),
  getTokenCount: () => getDeadlockGcTokenCount(),
});
const gcBuildSearch = new GcBuildSearch({
  client,
  log: (level, msg, extra) => log(level, msg, extra),
  trace: writeDeadlockGcTrace,
  db,
  appId: DEADLOCK_APP_ID,
});
const gcProfileCard = new GcProfileCard({
  client,
  log: (level, msg, extra) => log(level, msg, extra),
  trace: writeDeadlockGcTrace,
  appId: DEADLOCK_APP_ID,
});
let buildCatalogManager = null;
client.setOption('autoRelogin', false);
client.setOption('machineName', process.env.STEAM_MACHINE_NAME || 'DeadlockBridge');
const statusAnzeige = new StatusAnzeige(client, log, {
  appId: DEADLOCK_APP_ID,
  language: process.env.STEAM_PRESENCE_LANGUAGE || 'german',
  db,
  steamApiKey: STEAM_API_KEY,
  webApiTimeoutMs: WEB_API_HTTP_TIMEOUT_MS,
  webSummaryCacheTtlMs: WEB_API_FRIEND_CACHE_TTL_MS,
});
log('info', 'Statusanzeige initialisiert', { persistence: statusAnzeige.persistenceEnabled, pollIntervalMs: statusAnzeige.pollIntervalMs });
statusAnzeige.start();
buildCatalogManager = new BuildCatalogManager({
  db,
  gcBuildSearch,
  log: (level, msg, extra) => log(level, msg, extra),
  trace: writeDeadlockGcTrace,
  getPersonaName: async (accountId) => getPersonaName(accountId),
});
log('info', 'BuildCatalogManager initialisiert');

// ---------- Domain Modules ----------
// sharedCtx is a mutable object; late-bound properties are set after each module creation.
const sharedCtx = {
  state, runtimeState, client, log, SteamUser, SteamID,
  deadlockGcBot, gcBuildSearch, gcProfileCard, buildCatalogManager,
  getPersonaName, getWorkingAppId, normalizeToBuffer, convertKeysToCamelCase,
  getDeadlockGcTokenCount, requestDeadlockGcTokens,
  writeDeadlockGcTrace,
  nowSeconds, sleep, toPositiveInt,
  normalizeTimeoutMs, normalizeAttempts, isTimeoutError,
  readToken, writeToken,
  ACCOUNT_NAME, ACCOUNT_PASSWORD,
  DEADLOCK_APP_ID, DEADLOCK_APP_IDS,
  PROTO_MASK,
  GC_MSG_CLIENT_HELLO, GC_MSG_CLIENT_HELLO_ALT, GC_MSG_CLIENT_WELCOME,
  GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD, GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD_RESPONSE,
  GC_CLIENT_HELLO_PROTOCOL_VERSION,
  playtestMsgConfigs, buildPlaytestPayloadOverrideFn,
  DEADLOCK_GC_PROTOCOL_OVERRIDE_PATH, getHelloPayloadOverride,
  RECONNECT_DELAY_MS,
  MIN_GC_READY_TIMEOUT_MS, DEFAULT_GC_READY_TIMEOUT_MS, DEFAULT_GC_READY_ATTEMPTS, GC_READY_RETRY_DELAY_MS,
  MIN_PLAYTEST_INVITE_TIMEOUT_MS, DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS,
  DEFAULT_PLAYTEST_INVITE_ATTEMPTS, PLAYTEST_RETRY_DELAY_MS, INVITE_RESPONSE_MIN_TIMEOUT_MS,
  STEAM_API_KEY, WEB_API_HTTP_TIMEOUT_MS, WEB_API_FRIEND_CACHE_TTL_MS,
  REFRESH_TOKEN_PATH, MACHINE_TOKEN_PATH,
  STEAM_VAULT_REFRESH_TOKEN, STEAM_VAULT_MACHINE_TOKEN, STEAM_TOKEN_VAULT_ENABLED,
  COMMAND_BOT_KEY,
  FRIEND_REQUEST_BATCH_SIZE, FRIEND_REQUEST_RETRY_SECONDS, FRIEND_REQUEST_DAILY_CAP,
  STALE_TASK_TIMEOUT_S,
  selectPendingTaskStmt, markTaskRunningStmt, resetTaskPendingStmt, failStaleTasksStmt, finishTaskStmt,
  selectHeroBuildSourceStmt, selectHeroBuildCloneMetaStmt, updateHeroBuildCloneUploadedStmt,
  selectPendingCommandStmt, markCommandRunningStmt, finalizeCommandStmt,
  upsertStandaloneStateStmt, steamTaskCountsStmt, steamTaskRecentStmt,
  steamLinksForSyncStmt,
  selectFriendCheckCacheStmt, upsertFriendCheckCacheStmt, countFriendRequestsSentSinceStmt,
  upsertPendingFriendRequestStmt, selectFriendRequestBatchStmt,
  markFriendRequestSentStmt, markFriendRequestSkippedStmt, markFriendRequestFailedStmt,
  deleteFriendRequestStmt, clearFriendFlagStmt,
  verifySteamLinkStmt, unverifySteamLinkStmt,
};

// 1. State — provides scheduleStatePublish (late-bound into sharedCtx for other modules)
const stateModule = require('./src/state')(sharedCtx);
sharedCtx.scheduleStatePublish = stateModule.scheduleStatePublish;
const {
  safeJsonStringify, safeJsonParse, safeNumber, wrapOk, truncateError,
  getStatusPayload, buildStandaloneSnapshot, publishStandaloneState,
  scheduleStatePublish, completeTask,
} = stateModule;

// 2. Build Publisher
const buildPublisher = require('./src/build_publisher')(sharedCtx);
const {
  getUpdateHeroBuildResponseMsg, loadHeroBuildProto,
  cleanBuildDetails, composeBuildDescription,
  buildUpdateHeroBuild, buildMinimalHeroBuild, mapHeroBuildFromRow, sendHeroBuildUpdate,
} = buildPublisher;

// 3. GC Connection (late-binds ctx.initiateLogin after auth is created)
const gcConn = require('./src/gc_connection')(sharedCtx);
const {
  clearReconnectTimer, scheduleReconnect, ensureDeadlockGamePlaying, sendDeadlockGcHello,
  tryAlternativeGcHandshake, removeGcWaiter, flushDeadlockGcWaiters, notifyDeadlockGcReady,
  getDeadlockGcHelloPayload, createDeadlockGcReadyPromise, waitForDeadlockGcReady,
} = gcConn;
sharedCtx.waitForDeadlockGcReady = waitForDeadlockGcReady;
sharedCtx.clearReconnectTimer = clearReconnectTimer;

// 4. Auth (needs clearReconnectTimer from gcConn; late-binds ctx.scheduleStatePublish)
const auth = require('./src/auth')(sharedCtx);
const {
  updateRefreshToken, updateMachineToken, guardTypeFromDomain, buildLoginOptions,
  initiateLogin, handleGuardCodeTask, handleLogoutTask,
} = auth;
sharedCtx.initiateLogin = initiateLogin;

// 5. Friends
const friends = require('./src/friends')(sharedCtx);
const {
  normalizeSteamId64, parseSteamID, relationshipName,
  loadWebApiFriendIds, isFriendViaWebApi, isAlreadyFriend, getWebApiFriendCacheAgeMs,
  sendFriendRequest, removeFriendship, verifySteamLink,
  requestProfileCardForSid, queueFriendRequestForId, collectKnownFriendIds,
  resolveCachedPersonaName, fetchPersonaNames, syncFriendsAndLinks,
} = friends;

// 6. Playtest (late-binds ctx.waitForDeadlockGcReady, already set above)
const playtest = require('./src/playtest')(sharedCtx);
const {
  formatPlaytestError, encodeSubmitPlaytestUserPayload,
  removePendingPlaytestInvite, flushPendingPlaytestInvites,
  sendPlaytestInvite, sendPlaytestInviteOnce, handlePlaytestInviteResponse,
} = playtest;

// ---------- Task Dispatcher ----------
const taskModule = require('./src/tasks')({
  state, runtimeState, client, log,
  deadlockGcBot, gcBuildSearch, gcProfileCard, buildCatalogManager,
  SteamUser, nowSeconds,
  STALE_TASK_TIMEOUT_S, TASK_POLL_INTERVAL_MS, FRIEND_SYNC_INTERVAL_MS,
  failStaleTasksStmt, selectPendingTaskStmt, markTaskRunningStmt,
  resetTaskPendingStmt, finishTaskStmt,
  selectHeroBuildSourceStmt, selectHeroBuildCloneMetaStmt, updateHeroBuildCloneUploadedStmt,
  GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD, GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD_RESPONSE,
  GC_MSG_FIND_HERO_BUILDS_RESPONSE, PROTO_MASK,
  buildUpdateHeroBuild, buildMinimalHeroBuild, mapHeroBuildFromRow, sendHeroBuildUpdate,
  safeJsonParse, safeNumber, safeJsonStringify, wrapOk, truncateError,
  buildPlaytestPayloadOverrideFn, playtestMsgConfigs,
  DEFAULT_GC_READY_TIMEOUT_MS, DEFAULT_GC_READY_ATTEMPTS,
  requestDeadlockGcTokens, waitForDeadlockGcReady, sendDeadlockGcHello, ensureDeadlockGamePlaying,
  sendPlaytestInvite, sendPlaytestInviteOnce, handlePlaytestInviteResponse,
  parseSteamID, isAlreadyFriend, isFriendViaWebApi, getWebApiFriendCacheAgeMs,
  removeFriendship, sendFriendRequest,
  initiateLogin, handleGuardCodeTask, handleLogoutTask,
  getStatusPayload, buildStandaloneSnapshot, publishStandaloneState, scheduleStatePublish, completeTask,
  syncFriendsAndLinks, collectKnownFriendIds, resolveCachedPersonaName,
  queueFriendRequestForId, requestProfileCardForSid,
  STEAM_API_KEY, WEB_API_HTTP_TIMEOUT_MS, WEB_API_FRIEND_CACHE_TTL_MS,
});
const { finalizeTaskRun, processNextTask } = taskModule;

// ---------- Standalone Command Handling ----------
const commandModule = require('./src/commands')({
  state, runtimeState, log,
  initiateLogin, scheduleStatePublish, getStatusPayload,
  syncFriendsAndLinks, collectKnownFriendIds, resolveCachedPersonaName,
  requestProfileCardForSid, queueFriendRequestForId,
  selectPendingCommandStmt, markCommandRunningStmt, finalizeCommandStmt,
  COMMAND_BOT_KEY, COMMAND_POLL_INTERVAL_MS, STATE_PUBLISH_INTERVAL_MS, GC_UNHEALTHY_THRESHOLD_MS, shutdown, nowSeconds,
  wrapOk, safeJsonParse, safeJsonStringify, truncateError,
  handleLogoutTask,
});
// ---------- Steam Events ----------
require('./src/events')({
  ...stateModule,
  ...gcConn,
  ...auth,
  ...friends,
  ...playtest,
  ...buildPublisher,
  state, runtimeState, client, log,
  SteamUser, statusAnzeige, deadlockGcBot, gcBuildSearch, gcProfileCard, buildCatalogManager,
  DEADLOCK_APP_ID, DEADLOCK_APP_IDS, PROTO_MASK,
  GC_MSG_CLIENT_HELLO, GC_MSG_CLIENT_HELLO_ALT, GC_MSG_CLIENT_WELCOME,
  GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD, GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD_RESPONSE,
  GC_CLIENT_HELLO_PROTOCOL_VERSION,
  playtestMsgConfigs, buildPlaytestPayloadOverrideFn,
  REFRESH_TOKEN_PATH, MACHINE_TOKEN_PATH, STEAM_VAULT_REFRESH_TOKEN, STEAM_VAULT_MACHINE_TOKEN, STEAM_TOKEN_VAULT_ENABLED,
  writeToken, writeDeadlockGcTrace, requestDeadlockGcTokens, getDeadlockGcTokenCount,
  GC_MSG_FIND_HERO_BUILDS_RESPONSE,
  unverifySteamLinkStmt,
  nowSeconds,
});

// ---------- Startup ----------
function autoLoginIfPossible() {
  if (!state.tokens.refreshToken) {
    log('info', 'Auto-login disabled (no refresh token). Waiting for tasks.');
    scheduleStatePublish({ reason: 'auto_login_skipped' });
    return;
  }
  const result = initiateLogin('auto-start', {});
  log('info', 'Auto-login kick-off', result);
  scheduleStatePublish({ reason: 'auto_login', started: Boolean(result?.started) });
}
autoLoginIfPossible();
publishStandaloneState({ reason: 'startup' });

// ---------- Build Catalog Maintenance ----------
const CATALOG_MAINTENANCE_INTERVAL_MS = parseInt(process.env.CATALOG_MAINTENANCE_INTERVAL_MS || '600000', 10);
function scheduleCatalogMaintenance() {
  try {
    const existing = db.prepare(
      `SELECT id FROM steam_tasks WHERE type = 'MAINTAIN_BUILD_CATALOG' AND status IN ('PENDING', 'RUNNING') LIMIT 1`
    ).get();
    if (!existing) {
      const now = Math.floor(Date.now() / 1000);
      db.prepare(`INSERT INTO steam_tasks (type, payload, status, created_at, updated_at) VALUES ('MAINTAIN_BUILD_CATALOG', '{}', 'PENDING', ?, ?)`).run(now, now);
      log('info', 'Scheduled MAINTAIN_BUILD_CATALOG task');
    } else {
      log('debug', 'MAINTAIN_BUILD_CATALOG task already scheduled', { task_id: existing.id });
    }
  } catch (err) {
    log('error', 'Failed to schedule catalog maintenance', { error: err.message });
  }
}
setTimeout(() => { log('info', 'Running initial catalog maintenance'); scheduleCatalogMaintenance(); }, 30000);
setInterval(() => { scheduleCatalogMaintenance(); }, CATALOG_MAINTENANCE_INTERVAL_MS);
setInterval(() => {
  if (runtimeState.logged_on) {
    log('debug', 'Running periodic game version check');
    deadlockGcBot.refreshGameVersion().catch(err => {
      log('warn', 'Periodic game version check failed', { error: err.message });
    });
  }
}, 30 * 60 * 1000);

// ---------- Shutdown ----------
function shutdown(code = 0) {
  try {
    log('info', 'Shutting down Steam bridge');
    statusAnzeige.stop();
    flushPendingPlaytestInvites(new Error('Service shutting down'));
    flushDeadlockGcWaiters(new Error('Service shutting down'));
    gcProfileCard.flushPending(new Error('Service shutting down'));
    client.logOff();
  } catch (err) {
    log('warn', 'Error during shutdown cleanup', { error: err && err.message ? err.message : String(err) });
  }
  try { db.close(); } catch (err) {
    log('warn', 'Failed to close database during shutdown', { error: err && err.message ? err.message : String(err) });
  }
  process.exit(code);
}
process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));
process.on('uncaughtException', (err) => {
  if (err && err.code === 'EPIPE') return;
  log('error', 'Uncaught exception', { error: err && err.stack ? err.stack : err });
  shutdown(1);
});
process.on('unhandledRejection', (err) => {
  log('error', 'Unhandled rejection', { error: err && err.stack ? err.stack : String(err) });
});
