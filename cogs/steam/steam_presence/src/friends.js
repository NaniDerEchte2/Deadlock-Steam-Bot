'use strict';

const https = require('https');
const { URL } = require('url');

/**
 * Friends — WebAPI friend cache, friend requests, DB sync
 * Context: { state, runtimeState, client, log, SteamUser, nowSeconds,
 *            gcProfileCard, STEAM_API_KEY, WEB_API_HTTP_TIMEOUT_MS,
 *            WEB_API_FRIEND_CACHE_TTL_MS, FRIEND_REQUEST_BATCH_SIZE,
 *            FRIEND_REQUEST_RETRY_SECONDS, FRIEND_REQUEST_DAILY_CAP,
 *            selectFriendCheckCacheStmt, upsertFriendCheckCacheStmt,
 *            steamLinksForSyncStmt, upsertPendingFriendRequestStmt,
 *            selectFriendRequestBatchStmt, markFriendRequestSentStmt,
 *            markFriendRequestFailedStmt, markFriendRequestSkippedStmt,
 *            deleteFriendRequestStmt, clearFriendFlagStmt,
 *            verifySteamLinkStmt, countFriendRequestsSentSinceStmt }
 */
module.exports = (ctx) => {
  const {
    state, runtimeState, client, log, SteamUser, nowSeconds,
    gcProfileCard, STEAM_API_KEY, WEB_API_HTTP_TIMEOUT_MS,
    WEB_API_FRIEND_CACHE_TTL_MS, FRIEND_REQUEST_BATCH_SIZE,
    FRIEND_REQUEST_RETRY_SECONDS, FRIEND_REQUEST_DAILY_CAP,
    selectFriendCheckCacheStmt, upsertFriendCheckCacheStmt,
    steamLinksForSyncStmt, upsertPendingFriendRequestStmt,
    selectFriendRequestBatchStmt, markFriendRequestSentStmt,
    markFriendRequestFailedStmt, markFriendRequestSkippedStmt,
    deleteFriendRequestStmt, clearFriendFlagStmt,
    verifySteamLinkStmt, countFriendRequestsSentSinceStmt,
  } = ctx;

  const FRIEND_REQUEST_DAILY_WINDOW_SEC = 24 * 60 * 60;
  const FRIEND_CHECK_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

  // ---------- Utils ----------
  function normalizeSteamId64(value) {
    if (!value) return null;
    const sid = String(value).trim();
    if (!sid) return null;
    if (!/^\d{5,}$/.test(sid)) return null;
    return sid;
  }

  function parseSteamID(input) {
    const SteamID = ctx.SteamID;
    if (!input) throw new Error('SteamID erforderlich');
    try {
      const sid = new SteamID(String(input));
      if (!sid.isValid()) throw new Error('Ungültige SteamID');
      return sid;
    } catch (err) {
      throw new Error(`Ungültige SteamID: ${err && err.message ? err.message : String(err)}`);
    }
  }

  function relationshipName(code) {
    if (code === undefined || code === null) return 'unknown';
    for (const [name, value] of Object.entries(SteamUser.EFriendRelationship || {})) {
      if (Number(value) === Number(code)) return name;
    }
    return String(code);
  }

  // ---------- HTTP util (local, for WebAPI) ----------
  function httpGetJson(url, timeoutMs = WEB_API_HTTP_TIMEOUT_MS) {
    return new Promise((resolve, reject) => {
      try {
        const req = https.request(url, {
          method: 'GET',
          headers: { 'User-Agent': 'DeadlockSteamBridge/1.0 (+steam_presence)', Accept: 'application/json' },
        }, (res) => {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(chunk));
          res.on('end', () => {
            const body = Buffer.concat(chunks);
            const text = body.toString('utf8');
            if (res.statusCode < 200 || res.statusCode >= 300) {
              const err = new Error(`HTTP ${res.statusCode}`);
              err.statusCode = res.statusCode;
              err.body = text;
              return reject(err);
            }
            if (!text) { resolve(null); return; }
            try { resolve(JSON.parse(text)); } catch (err) { err.body = text; reject(err); }
          });
        });
        req.on('error', reject);
        req.setTimeout(Math.max(1000, timeoutMs || WEB_API_HTTP_TIMEOUT_MS), () => {
          req.destroy(new Error('Request timed out'));
        });
        req.end();
      } catch (err) { reject(err); }
    });
  }

  // ---------- WebAPI Friend Cache ----------
  async function loadWebApiFriendIds(force = false) {
    if (!STEAM_API_KEY) {
      if (!state.webApiFriendCacheWarned) {
        state.webApiFriendCacheWarned = true;
        log('debug', 'Steam API key not configured - friendship fallback disabled');
      }
      return null;
    }
    if (!client || !client.steamID) return null;

    const now = Date.now();
    if (!force && state.webApiFriendCacheIds && now - state.webApiFriendCacheLastLoadedAt < WEB_API_FRIEND_CACHE_TTL_MS) {
      return state.webApiFriendCacheIds;
    }
    if (state.webApiFriendCachePromise) return state.webApiFriendCachePromise;

    const url = new URL('https://api.steampowered.com/ISteamUser/GetFriendList/v1/');
    url.searchParams.set('key', STEAM_API_KEY);
    url.searchParams.set('steamid', client.steamID.getSteamID64());
    url.searchParams.set('relationship', 'friend');

    state.webApiFriendCachePromise = httpGetJson(url.toString(), WEB_API_HTTP_TIMEOUT_MS)
      .then((body) => {
        const entries = body && body.friendslist && Array.isArray(body.friendslist.friends)
          ? body.friendslist.friends : [];
        const set = new Set();
        for (const entry of entries) {
          const sid = entry && entry.steamid ? String(entry.steamid).trim() : '';
          if (sid) set.add(sid);
        }
        state.webApiFriendCacheIds = set;
        state.webApiFriendCacheLastLoadedAt = Date.now();
        log('debug', 'Refreshed Steam Web API friend cache', { count: set.size, ttlMs: WEB_API_FRIEND_CACHE_TTL_MS });
        return set;
      })
      .catch((err) => {
        log('warn', 'Steam Web API friend list request failed', {
          error: err && err.message ? err.message : String(err),
          statusCode: err && err.statusCode ? err.statusCode : undefined,
        });
        return null;
      })
      .finally(() => { state.webApiFriendCachePromise = null; });

    return state.webApiFriendCachePromise;
  }

  async function isFriendViaWebApi(steamId64) {
    const normalized = String(steamId64 || '').trim();
    if (!normalized) return { friend: false, source: 'webapi', refreshed: false };

    let ids = await loadWebApiFriendIds(false);
    if (ids && ids.has(normalized)) return { friend: true, source: 'webapi-cache', refreshed: false };

    ids = await loadWebApiFriendIds(true);
    if (ids && ids.has(normalized)) return { friend: true, source: 'webapi-refresh', refreshed: true };

    return { friend: false, source: 'webapi', refreshed: true };
  }

  async function isAlreadyFriend(steamId64) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return false;

    const nowMs = Date.now();
    const cached = state.friendCheckCache.get(sid);
    if (cached && nowMs - cached.ts < FRIEND_CHECK_CACHE_TTL_MS) return cached.friend;

    try {
      const row = selectFriendCheckCacheStmt.get(sid);
      if (row && Number.isFinite(row.checked_at)) {
        const ageMs = nowMs - Number(row.checked_at) * 1000;
        if (ageMs < FRIEND_CHECK_CACHE_TTL_MS) {
          const isFriend = Boolean(row.friend);
          state.friendCheckCache.set(sid, { friend: isFriend, ts: nowMs });
          return isFriend;
        }
      }
    } catch (err) {
      log('debug', 'Friend cache DB lookup failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
    }

    const relMap = SteamUser.EFriendRelationship || {};
    const friendCode = Number(relMap.Friend);

    if (client && client.myFriends) {
      const rel = client.myFriends[sid];
      if (Number(rel) === friendCode) {
        state.friendCheckCache.set(sid, { friend: true, ts: nowMs });
        try { upsertFriendCheckCacheStmt.run(sid, 1, Math.floor(nowMs / 1000)); } catch (_) {}
        return true;
      }
    }

    try {
      const viaWeb = await isFriendViaWebApi(sid);
      if (viaWeb && viaWeb.friend) {
        state.friendCheckCache.set(sid, { friend: true, ts: nowMs });
        try { upsertFriendCheckCacheStmt.run(sid, 1, Math.floor(nowMs / 1000)); } catch (_) {}
        return true;
      }
    } catch (_) {}

    state.friendCheckCache.set(sid, { friend: false, ts: nowMs });
    try { upsertFriendCheckCacheStmt.run(sid, 0, Math.floor(nowMs / 1000)); } catch (_) {}
    return false;
  }

  function getWebApiFriendCacheAgeMs() {
    if (!state.webApiFriendCacheLastLoadedAt) return null;
    return Math.max(0, Date.now() - state.webApiFriendCacheLastLoadedAt);
  }

  // ---------- Friend Operations ----------
  async function sendFriendRequest(steamId) {
    const sid64 = steamId && typeof steamId.getSteamID64 === 'function'
      ? steamId.getSteamID64()
      : normalizeSteamId64(steamId);
    if (!sid64) throw new Error('Invalid SteamID');

    if (await isAlreadyFriend(sid64)) {
      log('info', 'Friend request skipped (already friends)', { steam_id64: sid64 });
      return true;
    }

    return new Promise((resolve, reject) => {
      try {
        client.addFriend(steamId, (err) => {
          if (err) {
            if (err.message && err.message.includes('DuplicateName')) {
              log('debug', 'Friend request duplicate - treating as already friends', { steam_id64: sid64 });
              return resolve(true);
            }
            return reject(err);
          }
          resolve(true);
        });
      } catch (err) { reject(err); }
    });
  }

  async function removeFriendship(steamInput) {
    let parsed;
    try {
      parsed = parseSteamID(steamInput);
    } catch (err) {
      throw new Error(`Ungültige SteamID: ${err && err.message ? err.message : String(err)}`);
    }

    const sid64 = typeof parsed.getSteamID64 === 'function' ? parsed.getSteamID64() : String(parsed);
    const previousRel = client && client.myFriends ? client.myFriends[sid64] : undefined;

    if (!client || typeof client.removeFriend !== 'function') {
      throw new Error('removeFriend not supported by steam client');
    }

    try {
      client.removeFriend(parsed);
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      log('warn', 'Failed to remove friend', { steam_id64: sid64, error: message });
      throw new Error(message);
    }

    try { deleteFriendRequestStmt.run(sid64); } catch (err) {
      log('debug', 'Failed to delete steam_friend_requests row after removal', { steam_id64: sid64, error: err && err.message ? err.message : String(err) });
    }
    try { clearFriendFlagStmt.run(sid64); } catch (err) {
      log('debug', 'Failed to clear steam_links flags after removal', { steam_id64: sid64, error: err && err.message ? err.message : String(err) });
    }

    return {
      steam_id64: sid64,
      account_id: parsed.accountid ?? null,
      previous_relationship: relationshipName(previousRel),
    };
  }

  function verifySteamLink(steamId64, displayName) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return false;
    const name = displayName ? String(displayName).trim() : '';
    try {
      const info = verifySteamLinkStmt.run({ steam_id: sid, name });
      return info.changes > 0;
    } catch (err) {
      log('warn', 'Failed to verify steam link', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
      return false;
    }
  }

  const profileCardThrottle = new Map();

  function requestProfileCardForSid(steamId64) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return;

    const now = nowSeconds();
    const last = profileCardThrottle.get(sid) || 0;
    if (now - last < 600) return;
    profileCardThrottle.set(sid, now);

    try {
      const SteamID = ctx.SteamID;
      const parsed = parseSteamID(sid);
      const accountId = parsed?.accountid ? Number(parsed.accountid) : null;
      if (!Number.isFinite(accountId) || accountId <= 0) return;

      gcProfileCard.fetchPlayerCard({ accountId, timeoutMs: 15000, friendAccessHint: true })
        .catch((err) => {
          log('debug', 'ProfileCard prefetch failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
        });
    } catch (err) {
      log('debug', 'ProfileCard prefetch parse failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
    }
  }

  function queueFriendRequestForId(steamId64) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return false;
    try {
      upsertPendingFriendRequestStmt.run(sid);
      return true;
    } catch (err) {
      log('warn', 'Failed to queue Steam friend request', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
      return false;
    }
  }

  // ---------- Friend Sync ----------
  async function collectKnownFriendIds() {
    const ids = new Set();
    const relMap = SteamUser.EFriendRelationship || {};
    const friendCode = Number(relMap.Friend);
    let clientCount = 0;
    let webCount = 0;

    if (client && client.myFriends && Object.keys(client.myFriends).length) {
      for (const [sid, rel] of Object.entries(client.myFriends)) {
        if (Number(rel) === friendCode) {
          const norm = normalizeSteamId64(sid);
          if (norm) { ids.add(norm); clientCount += 1; }
        }
      }
    }

    try {
      const webIds = await loadWebApiFriendIds(false);
      if (webIds && webIds.size) {
        webIds.forEach((sid) => { const norm = normalizeSteamId64(sid); if (norm) ids.add(norm); });
        webCount = webIds.size;
      }
    } catch (err) {
      log('debug', 'Friend sync: failed to load WebAPI friend list', { error: err && err.message ? err.message : String(err) });
    }

    return { ids, clientCount, webCount };
  }

  function resolveCachedPersonaName(steamId64) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid || !client || !client.users) return '';
    try {
      const cached = client.users[sid];
      if (cached && cached.player_name) return String(cached.player_name).trim();
    } catch (_) {}
    return '';
  }

  async function fetchPersonaNames(steamIds) {
    const SteamID = ctx.SteamID;
    const result = new Map();
    const normalized = Array.from(new Set(
      Array.from(steamIds || []).map((sid) => normalizeSteamId64(sid)).filter(Boolean)
    ));
    if (!normalized.length) return result;

    for (const sid of normalized) {
      const cached = resolveCachedPersonaName(sid);
      if (cached) result.set(sid, cached);
    }

    const remaining = normalized.filter((sid) => !result.has(sid));
    if (!remaining.length || !client || typeof client.getPersonas !== 'function') return result;

    try {
      const personaArgs = remaining.map((sid) => { try { return new SteamID(sid); } catch (_) { return sid; } });
      const personas = await client.getPersonas(personaArgs);
      if (personas && typeof personas === 'object') {
        for (const [sidKey, persona] of Object.entries(personas)) {
          const sid = normalizeSteamId64(sidKey);
          const name = persona && persona.player_name ? String(persona.player_name).trim() : '';
          if (sid && name) result.set(sid, name);
        }
      }
    } catch (err) {
      log('debug', 'Friend sync: failed to load persona names', { error: err && err.message ? err.message : String(err) });
    }

    return result;
  }

  async function processFriendRequestQueue(currentFriends, reason) {
    const outcome = { sent: 0, failed: 0, skipped: 0 };
    if (!runtimeState.logged_on) return outcome;

    const rows = selectFriendRequestBatchStmt.all(FRIEND_REQUEST_BATCH_SIZE);
    const now = nowSeconds();
    let sentLast24h = 0;
    try {
      const row = countFriendRequestsSentSinceStmt.get(now - FRIEND_REQUEST_DAILY_WINDOW_SEC);
      sentLast24h = row && typeof row.c === 'number' ? row.c : 0;
    } catch (err) {
      log('warn', 'Failed to count recent friend requests', { error: err && err.message ? err.message : String(err) });
    }

    for (const row of rows) {
      if (FRIEND_REQUEST_DAILY_CAP > 0 && sentLast24h >= FRIEND_REQUEST_DAILY_CAP) {
        log('info', 'Friend request daily cap reached - skipping remaining queue', { cap: FRIEND_REQUEST_DAILY_CAP, sent_last_24h: sentLast24h, reason });
        break;
      }

      const sid = normalizeSteamId64(row.steam_id);
      if (!sid) continue;

      if (currentFriends && currentFriends.has(sid)) {
        try { markFriendRequestSkippedStmt.run(now, sid); outcome.skipped += 1; } catch (err) {
          log('debug', 'Failed to mark existing friend request as sent', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
        }
        continue;
      }

      try {
        if (await isAlreadyFriend(sid)) {
          markFriendRequestSkippedStmt.run(now, sid);
          outcome.skipped += 1;
          continue;
        }
      } catch (err) {
        log('debug', 'Friend pre-check failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
      }

      if (row.last_attempt && now - Number(row.last_attempt) < FRIEND_REQUEST_RETRY_SECONDS) continue;

      let parsed;
      try { parsed = parseSteamID(sid); } catch (err) {
        markFriendRequestFailedStmt.run(now, err && err.message ? err.message : String(err), sid);
        outcome.failed += 1;
        continue;
      }

      try {
        await sendFriendRequest(parsed);
        markFriendRequestSentStmt.run(now, sid);
        outcome.sent += 1;
        sentLast24h += 1;
      } catch (err) {
        const message = err && err.message ? err.message : String(err);
        markFriendRequestFailedStmt.run(now, message, sid);
        outcome.failed += 1;
        log('warn', 'Friend request failed', { steam_id64: sid, error: message, reason });
      }
    }

    return outcome;
  }

  async function syncFriendsAndLinks(reason = 'interval') {
    if (state.friendSyncInProgress) return;
    if (!runtimeState.logged_on || !client || !client.steamID) return;

    state.friendSyncInProgress = true;
    try {
      const { ids: friendIds, clientCount, webCount } = await collectKnownFriendIds();
      const ownSid = normalizeSteamId64(
        runtimeState.steam_id64 ||
        (client.steamID && typeof client.steamID.getSteamID64 === 'function' ? client.steamID.getSteamID64() : null)
      );
      if (ownSid) friendIds.delete(ownSid);

      const dbRows = steamLinksForSyncStmt.all();
      const dbSteamIds = new Set();
      const missingNameIds = new Set();

      for (const row of dbRows) {
        const sid = normalizeSteamId64(row.steam_id);
        if (!sid || (ownSid && sid === ownSid)) continue;
        dbSteamIds.add(sid);
        if (Number(row.user_id) === 0) {
          const nameRaw = typeof row.name === 'string' ? row.name : '';
          if (!nameRaw || !String(nameRaw).trim()) missingNameIds.add(sid);
        }
      }

      const idsNeedingName = new Set();
      for (const sid of friendIds) {
        if (ownSid && sid === ownSid) continue;
        if (!dbSteamIds.has(sid) || missingNameIds.has(sid)) idsNeedingName.add(sid);
      }
      const personaNames = await fetchPersonaNames(idsNeedingName);

      let nameUpdates = 0;
      for (const sid of friendIds) {
        if (ownSid && sid === ownSid) continue;
        const needsName = missingNameIds.has(sid);
        const name = personaNames.get(sid) || '';
        if (!dbSteamIds.has(sid)) continue;
        if (needsName && name) { verifySteamLink(sid, name); nameUpdates += 1; }
      }

      const requestOutcome = await processFriendRequestQueue(friendIds, reason);

      if (nameUpdates || requestOutcome.sent || requestOutcome.failed) {
        log('info', 'Friend/DB sync completed', {
          reason, name_updates: nameUpdates,
          friend_requests_sent: requestOutcome.sent,
          friend_requests_failed: requestOutcome.failed,
          friends_known: friendIds.size, links_known: dbSteamIds.size,
          friend_sources: { client: clientCount, webapi: webCount },
        });
      } else {
        log('debug', 'Friend/DB sync done (no changes)', {
          reason, friends_known: friendIds.size, links_known: dbSteamIds.size,
          friend_sources: { client: clientCount, webapi: webCount },
        });
      }
    } catch (err) {
      log('warn', 'Friend/DB sync failed', { reason, error: err && err.message ? err.message : String(err) });
    } finally {
      state.friendSyncInProgress = false;
    }
  }

  return {
    normalizeSteamId64,
    parseSteamID,
    relationshipName,
    loadWebApiFriendIds,
    isFriendViaWebApi,
    isAlreadyFriend,
    getWebApiFriendCacheAgeMs,
    sendFriendRequest,
    removeFriendship,
    verifySteamLink,
    requestProfileCardForSid,
    queueFriendRequestForId,
    collectKnownFriendIds,
    resolveCachedPersonaName,
    fetchPersonaNames,
    syncFriendsAndLinks,
  };
};
