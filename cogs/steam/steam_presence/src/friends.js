'use strict';

const https = require('https');
const { URL } = require('url');

/**
 * Friends — WebAPI friend cache, friend requests, DB sync
 * Context: { state, runtimeState, client, log, SteamUser, nowSeconds,
 *            gcProfileCard, gcScheduler, STEAM_API_KEY, WEB_API_HTTP_TIMEOUT_MS,
 *            WEB_API_FRIEND_CACHE_TTL_MS, FRIEND_REQUEST_BATCH_SIZE,
 *            FRIEND_REQUEST_RETRY_SECONDS, FRIEND_REQUEST_DAILY_CAP,
 *            STEAM_OUTGOING_FRIEND_REQUESTS_ENABLED,
 *            selectFriendCheckCacheStmt, upsertFriendCheckCacheStmt,
 *            steamLinksForSyncStmt, upsertPendingFriendRequestStmt,
 *            selectFriendRequestBatchStmt, markFriendRequestSentStmt,
 *            markFriendRequestFailedStmt, markFriendRequestSkippedStmt,
 *            deleteFriendRequestStmt, clearFriendFlagStmt,
 *            selectSteamLinkOwnersForSteamIdStmt,
 *            verifySteamLinkForUserStmt, unverifySteamLinkForUserStmt,
 *            selectActiveVerifiedFriendLinkForUserStmt,
 *            upsertRoleCleanupPendingStmt,
 *            countFriendRequestsSentSinceStmt }
 */
module.exports = (ctx) => {
  const {
    state, runtimeState, client, log, SteamUser, nowSeconds,
    gcProfileCard, gcScheduler, STEAM_API_KEY, WEB_API_HTTP_TIMEOUT_MS,
    WEB_API_FRIEND_CACHE_TTL_MS, FRIEND_REQUEST_BATCH_SIZE,
    FRIEND_REQUEST_RETRY_SECONDS, FRIEND_REQUEST_DAILY_CAP,
    STEAM_OUTGOING_FRIEND_REQUESTS_ENABLED,
    selectFriendCheckCacheStmt, upsertFriendCheckCacheStmt,
    steamLinksForSyncStmt, upsertPendingFriendRequestStmt,
    selectFriendRequestBatchStmt, markFriendRequestSentStmt,
    markFriendRequestFailedStmt, markFriendRequestSkippedStmt,
    deleteFriendRequestStmt, clearFriendFlagStmt,
    selectSteamLinkOwnersForSteamIdStmt,
    verifySteamLinkForUserStmt, unverifySteamLinkForUserStmt,
    selectActiveVerifiedFriendLinkForUserStmt, upsertRoleCleanupPendingStmt,
    countFriendRequestsSentSinceStmt,
  } = ctx;

  const FRIEND_REQUEST_DAILY_WINDOW_SEC = 24 * 60 * 60;
  const FRIEND_CHECK_CACHE_POSITIVE_TTL_MS = 24 * 60 * 60 * 1000;
  const FRIEND_CHECK_CACHE_NEGATIVE_TTL_MS = 2 * 60 * 1000;
  const FRIEND_REQUEST_ERROR_WARN_LIMIT_PER_BATCH = 5;
  const PROFILE_CARD_THROTTLE_WINDOW_S = 600;
  const CACHE_CLEANUP_INTERVAL_MS = 60 * 1000;

  function toBoundedInt(value, fallback, minimum, maximum) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return fallback;
    return Math.max(minimum, Math.min(maximum, Math.floor(parsed)));
  }

  const FRIEND_CHECK_CACHE_MAX_ENTRIES = toBoundedInt(
    process.env.STEAM_FRIEND_CHECK_CACHE_MAX_ENTRIES,
    20000,
    1000,
    200000
  );
  const PROFILE_CARD_THROTTLE_MAX_ENTRIES = toBoundedInt(
    process.env.STEAM_PROFILE_CARD_THROTTLE_MAX_ENTRIES,
    20000,
    1000,
    200000
  );

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

  function normalizeDiscordUserId(value) {
    const userId = String(value || '').trim();
    if (!/^\d+$/.test(userId) || userId === '0') return null;
    return userId;
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
  // Temporarily disabled: rely solely on GC/client data to avoid WebAPI 401 spam.
  const DISABLE_WEBAPI_FRIENDS = ['1', 'true', 'yes', 'on', 'y'].includes(
    String(process.env.STEAM_DISABLE_WEBAPI_FRIENDS || '1').toLowerCase()
  );

  async function loadWebApiFriendIds(force = false) { // eslint-disable-line no-unused-vars
    if (DISABLE_WEBAPI_FRIENDS) return null;
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

  async function isFriendViaWebApi(steamId64) { // eslint-disable-line no-unused-vars
    if (DISABLE_WEBAPI_FRIENDS) return { friend: false, source: 'webapi-disabled', refreshed: false };
    const normalized = String(steamId64 || '').trim();
    if (!normalized) return { friend: false, source: 'webapi', refreshed: false };

    let ids = await loadWebApiFriendIds(false);
    if (ids && ids.has(normalized)) return { friend: true, source: 'webapi-cache', refreshed: false };

    ids = await loadWebApiFriendIds(true);
    if (ids && ids.has(normalized)) return { friend: true, source: 'webapi-refresh', refreshed: true };

    return { friend: false, source: 'webapi', refreshed: true };
  }

  function getFriendCheckTtlMs(isFriend) {
    return isFriend ? FRIEND_CHECK_CACHE_POSITIVE_TTL_MS : FRIEND_CHECK_CACHE_NEGATIVE_TTL_MS;
  }

  let lastFriendCheckCacheCleanupAt = 0;
  function pruneFriendCheckCache(nowMs = Date.now(), force = false) {
    if (!(state.friendCheckCache instanceof Map)) return;
    if (!force && state.friendCheckCache.size <= FRIEND_CHECK_CACHE_MAX_ENTRIES) {
      if (nowMs - lastFriendCheckCacheCleanupAt < CACHE_CLEANUP_INTERVAL_MS) return;
    }

    let removedExpired = 0;
    for (const [sid, entry] of state.friendCheckCache.entries()) {
      const isFriend = Boolean(entry && entry.friend);
      const ts = Number(entry && entry.ts ? entry.ts : 0);
      const ttlMs = getFriendCheckTtlMs(isFriend);
      if (!Number.isFinite(ts) || nowMs - ts >= ttlMs) {
        state.friendCheckCache.delete(sid);
        removedExpired += 1;
      }
    }

    let removedOverflow = 0;
    let overflow = state.friendCheckCache.size - FRIEND_CHECK_CACHE_MAX_ENTRIES;
    while (overflow > 0) {
      const oldestKey = state.friendCheckCache.keys().next().value;
      if (oldestKey === undefined) break;
      state.friendCheckCache.delete(oldestKey);
      removedOverflow += 1;
      overflow -= 1;
    }

    lastFriendCheckCacheCleanupAt = nowMs;
    if ((removedExpired + removedOverflow) > 0) {
      log('debug', 'Pruned friend-check cache', {
        removed_expired: removedExpired,
        removed_overflow: removedOverflow,
        size: state.friendCheckCache.size,
      });
    }
  }

  function setFriendCheckCacheStatus(steamId64, isFriend, source = 'unknown') {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return false;
    const nowMs = Date.now();
    pruneFriendCheckCache(nowMs);
    const friend = Boolean(isFriend);
    state.friendCheckCache.set(sid, { friend, ts: nowMs });
    try {
      upsertFriendCheckCacheStmt.run(sid, friend ? 1 : 0, Math.floor(nowMs / 1000));
    } catch (err) {
      log('debug', 'Failed to persist friend-check cache status', {
        steam_id64: sid,
        friend,
        source,
        error: err && err.message ? err.message : String(err),
      });
    }
    return friend;
  }

  async function isAlreadyFriend(steamId64) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return false;

    const nowMs = Date.now();
    pruneFriendCheckCache(nowMs);
    const cached = state.friendCheckCache.get(sid);
    if (cached) {
      const ttlMs = getFriendCheckTtlMs(Boolean(cached.friend));
      if (nowMs - cached.ts < ttlMs) return Boolean(cached.friend);
      state.friendCheckCache.delete(sid);
    }

    try {
      const row = selectFriendCheckCacheStmt.get(sid);
      if (row && Number.isFinite(row.checked_at)) {
        const isFriend = Boolean(row.friend);
        const ageMs = nowMs - Number(row.checked_at) * 1000;
        if (ageMs < getFriendCheckTtlMs(isFriend)) {
          state.friendCheckCache.set(sid, {
            friend: isFriend,
            ts: Number(row.checked_at) * 1000,
          });
          return isFriend;
        }
      }
    } catch (err) {
      log('debug', 'Friend cache DB lookup failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
    }

    const relMap = SteamUser.EFriendRelationship || {};
    const friendCode = Number(relMap.Friend);

    if (client && client.myFriends) {
      const hasRel = Object.prototype.hasOwnProperty.call(client.myFriends, sid);
      if (hasRel) {
        return setFriendCheckCacheStatus(sid, Number(client.myFriends[sid]) === friendCode, 'client');
      }
    }

    try {
      const viaWeb = await isFriendViaWebApi(sid);
      if (viaWeb && viaWeb.friend) {
        return setFriendCheckCacheStatus(sid, true, viaWeb.source || 'webapi');
      }
    } catch (_) {}

    return setFriendCheckCacheStatus(sid, false, 'miss');
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

    const owner = resolveUniqueOwnerUserId(sid64, 'remove_friendship_cleanup');
    if (owner.ownerUserId) {
      queueRoleCleanupIfFullyUnfollowed(
        owner.ownerUserId,
        `Steam friendship removed manually (steam_id=${sid64})`
      );
    }

    return {
      steam_id64: sid64,
      account_id: parsed.accountid ?? null,
      previous_relationship: relationshipName(previousRel),
    };
  }

  function resolveUniqueOwnerUserId(steamId64, action) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return { steamId: null, ownerUserId: null };
    let ownerRows = [];
    try {
      ownerRows = selectSteamLinkOwnersForSteamIdStmt.all(sid);
    } catch (err) {
      log('warn', 'Failed to query steam_link owners before ownership-scoped update', {
        steam_id64: sid,
        action,
        error: err && err.message ? err.message : String(err),
      });
      return { steamId: sid, ownerUserId: null };
    }
    if (!Array.isArray(ownerRows) || ownerRows.length === 0) {
      log('debug', 'Skipping ownership-scoped update without user-owned steam_link row', {
        steam_id64: sid,
        action,
      });
      return { steamId: sid, ownerUserId: null };
    }
    if (ownerRows.length > 1) {
      log('warn', 'Blocked ownership-scoped update due to duplicate steam_id ownership', {
        steam_id64: sid,
        action,
        owner_user_ids: ownerRows.map((row) => row.user_id),
      });
      return { steamId: sid, ownerUserId: null };
    }
    // Keep Discord snowflakes as exact decimal strings (no JS number conversion).
    const ownerUserId = String(ownerRows[0].user_id || '').trim();
    if (!/^\d+$/.test(ownerUserId) || ownerUserId === '0') {
      log('warn', 'Skipping ownership-scoped update due to invalid owner user_id', {
        steam_id64: sid,
        action,
        owner_user_id: ownerRows[0].user_id,
      });
      return { steamId: sid, ownerUserId: null };
    }
    return { steamId: sid, ownerUserId };
  }

  function verifySteamLink(steamId64, displayName) {
    const owner = resolveUniqueOwnerUserId(steamId64, 'verify');
    if (!owner.steamId || !owner.ownerUserId) return false;
    const name = displayName ? String(displayName).trim() : '';
    try {
      const info = verifySteamLinkForUserStmt.run({
        steam_id: owner.steamId,
        user_id: owner.ownerUserId,
        name,
      });
      return info.changes > 0;
    } catch (err) {
      log('warn', 'Failed to verify steam link', {
        steam_id64: owner.steamId,
        owner_user_id: owner.ownerUserId,
        error: err && err.message ? err.message : String(err),
      });
      return false;
    }
  }

  function queueRoleCleanupIfFullyUnfollowed(userId, reason) {
    const ownerUserId = normalizeDiscordUserId(userId);
    if (!ownerUserId) {
      return { queued: false, activeFriendLinkRemains: false, ownerUserId: null };
    }
    try {
      if (selectActiveVerifiedFriendLinkForUserStmt.get(ownerUserId)) {
        return {
          queued: false,
          activeFriendLinkRemains: true,
          ownerUserId,
        };
      }
      const now = nowSeconds();
      upsertRoleCleanupPendingStmt.run(
        ownerUserId,
        String(reason || 'Steam unfollow cleanup'),
        now,
        now
      );
      return {
        queued: true,
        activeFriendLinkRemains: false,
        ownerUserId,
      };
    } catch (err) {
      log('warn', 'Failed to queue steam role cleanup', {
        owner_user_id: ownerUserId,
        reason,
        error: err && err.message ? err.message : String(err),
      });
      return {
        queued: false,
        activeFriendLinkRemains: false,
        ownerUserId,
        error: err,
      };
    }
  }

  function unverifySteamLink(steamId64, options = {}) {
    const owner = resolveUniqueOwnerUserId(steamId64, 'unverify');
    if (!owner.steamId || !owner.ownerUserId) {
      return {
        changed: false,
        queuedCleanup: false,
        ownerUserId: owner.ownerUserId || null,
        steamId: owner.steamId || null,
      };
    }

    let changed = false;
    try {
      const info = unverifySteamLinkForUserStmt.run({
        steam_id: owner.steamId,
        user_id: owner.ownerUserId,
      });
      changed = info.changes > 0;
    } catch (err) {
      log('warn', 'Failed to unverify steam link', {
        steam_id64: owner.steamId,
        owner_user_id: owner.ownerUserId,
        error: err && err.message ? err.message : String(err),
      });
      return {
        changed: false,
        queuedCleanup: false,
        ownerUserId: owner.ownerUserId,
        steamId: owner.steamId,
        error: err,
      };
    }

    const cleanupResult = queueRoleCleanupIfFullyUnfollowed(
      owner.ownerUserId,
      String(
        options.reason ||
        `Steam unfollow detected (source=${String(options.source || 'unknown')}, steam_id=${owner.steamId})`
      )
    );
    return {
      changed,
      queuedCleanup: Boolean(cleanupResult.queued),
      activeFriendLinkRemains: Boolean(cleanupResult.activeFriendLinkRemains),
      ownerUserId: owner.ownerUserId,
      steamId: owner.steamId,
    };
  }

  const profileCardThrottle = new Map();
  let lastProfileCardThrottleCleanupAt = 0;
  function pruneProfileCardThrottle(now = nowSeconds(), force = false) {
    if (!force && profileCardThrottle.size <= PROFILE_CARD_THROTTLE_MAX_ENTRIES) {
      if (now - lastProfileCardThrottleCleanupAt < 60) return;
    }

    let removedExpired = 0;
    for (const [sid, ts] of profileCardThrottle.entries()) {
      if (!Number.isFinite(ts) || (now - Number(ts)) >= PROFILE_CARD_THROTTLE_WINDOW_S) {
        profileCardThrottle.delete(sid);
        removedExpired += 1;
      }
    }

    let removedOverflow = 0;
    let overflow = profileCardThrottle.size - PROFILE_CARD_THROTTLE_MAX_ENTRIES;
    while (overflow > 0) {
      const oldestKey = profileCardThrottle.keys().next().value;
      if (oldestKey === undefined) break;
      profileCardThrottle.delete(oldestKey);
      removedOverflow += 1;
      overflow -= 1;
    }

    lastProfileCardThrottleCleanupAt = now;
    if ((removedExpired + removedOverflow) > 0) {
      log('debug', 'Pruned profile-card throttle cache', {
        removed_expired: removedExpired,
        removed_overflow: removedOverflow,
        size: profileCardThrottle.size,
      });
    }
  }

  function requestProfileCardForSid(steamId64) {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return;

    const now = nowSeconds();
    pruneProfileCardThrottle(now);
    const last = profileCardThrottle.get(sid) || 0;
    if (now - last < PROFILE_CARD_THROTTLE_WINDOW_S) return;
    profileCardThrottle.set(sid, now);

    try {
      const parsed = parseSteamID(sid);
      const accountId = parsed?.accountid ? Number(parsed.accountid) : null;
      if (!Number.isFinite(accountId) || accountId <= 0) return;

      const fetchPromise = gcScheduler && typeof gcScheduler.fetchProfileCard === 'function'
        ? gcScheduler.fetchProfileCard({
            type: 'profile_card_prefetch',
            accountId,
            timeoutMs: 15000,
            friendAccessHint: true,
            requireGcReady: true,
            gcReadyTimeoutMs: 15000,
            gcRetryAttempts: 1,
            timeoutRetries: 0,
          })
        : gcProfileCard.fetchPlayerCard({ accountId, timeoutMs: 15000, friendAccessHint: true });
      fetchPromise.catch((err) => {
        log('debug', 'ProfileCard prefetch failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
      });
    } catch (err) {
      log('debug', 'ProfileCard prefetch parse failed', { steam_id64: sid, error: err && err.message ? err.message : String(err) });
    }
  }

  function queueFriendRequestForId(steamId64) {
    if (!STEAM_OUTGOING_FRIEND_REQUESTS_ENABLED) return false;
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

  function resolveManualFriendRequest(steamId64, reason = 'incoming_accept') {
    const sid = normalizeSteamId64(steamId64);
    if (!sid) return false;
    try {
      deleteFriendRequestStmt.run(sid);
      return true;
    } catch (err) {
      log('warn', 'Failed to resolve manual Steam friend request row', {
        steam_id64: sid,
        reason,
        error: err && err.message ? err.message : String(err),
      });
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

    if (!DISABLE_WEBAPI_FRIENDS) {
      try {
        const webIds = await loadWebApiFriendIds(false);
        if (webIds && webIds.size) {
          webIds.forEach((sid) => { const norm = normalizeSteamId64(sid); if (norm) ids.add(norm); });
          webCount = webIds.size;
        }
      } catch (err) {
        log('debug', 'Friend sync: failed to load WebAPI friend list', { error: err && err.message ? err.message : String(err) });
      }
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
    if (!STEAM_OUTGOING_FRIEND_REQUESTS_ENABLED) {
      return outcome;
    }

    const rows = selectFriendRequestBatchStmt.all(FRIEND_REQUEST_BATCH_SIZE);
    const now = nowSeconds();
    let warnedFailures = 0;
    let suppressedFailureLogs = 0;
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
        if (warnedFailures < FRIEND_REQUEST_ERROR_WARN_LIMIT_PER_BATCH) {
          warnedFailures += 1;
          log('warn', 'Friend request failed', { steam_id64: sid, error: message, reason });
        } else {
          suppressedFailureLogs += 1;
        }
      }
    }

    if (suppressedFailureLogs > 0) {
      log('warn', 'Friend request failures suppressed for batch', {
        suppressed: suppressedFailureLogs,
        warn_limit: FRIEND_REQUEST_ERROR_WARN_LIMIT_PER_BATCH,
        failed_total: outcome.failed,
        reason,
      });
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
      const pendingCleanupByUser = new Map();
      let unfollowReconciled = 0;
      let cleanupQueued = 0;
      let cleanupSkipped = 0;

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

      for (const row of dbRows) {
        const sid = normalizeSteamId64(row.steam_id);
        if (!sid || (ownSid && sid === ownSid) || friendIds.has(sid)) continue;

        const ownerUserId = normalizeDiscordUserId(row.user_id);
        if (!ownerUserId) continue;

        const verified = Number(row.verified) === 1;
        const isSteamFriend = Number(row.is_steam_friend) === 1;
        if (verified || isSteamFriend) {
          const reconcileResult = unverifySteamLink(sid, {
            source: `syncFriendsAndLinks:${reason}`,
            reason: `Steam unfollow reconcile (${reason}, steam_id=${sid})`,
          });
          if (reconcileResult.changed) unfollowReconciled += 1;
          if (reconcileResult.queuedCleanup) cleanupQueued += 1;
          else if (reconcileResult.activeFriendLinkRemains) cleanupSkipped += 1;
          continue;
        }

        if (!pendingCleanupByUser.has(ownerUserId)) {
          pendingCleanupByUser.set(
            ownerUserId,
            `Steam cleanup reconcile (${reason}, steam_id=${sid})`
          );
        }
      }

      for (const [ownerUserId, cleanupReason] of pendingCleanupByUser.entries()) {
        const queueResult = queueRoleCleanupIfFullyUnfollowed(ownerUserId, cleanupReason);
        if (queueResult.queued) cleanupQueued += 1;
        else if (queueResult.activeFriendLinkRemains) cleanupSkipped += 1;
      }

      const requestOutcome = await processFriendRequestQueue(friendIds, reason);

      if (
        nameUpdates ||
        requestOutcome.sent ||
        requestOutcome.failed ||
        unfollowReconciled ||
        cleanupQueued
      ) {
        log('info', 'Friend/DB sync completed', {
          reason, name_updates: nameUpdates,
          friend_requests_sent: requestOutcome.sent,
          friend_requests_failed: requestOutcome.failed,
          unfollow_reconciled: unfollowReconciled,
          role_cleanup_queued: cleanupQueued,
          role_cleanup_skipped_active_friend: cleanupSkipped,
          friends_known: friendIds.size, links_known: dbSteamIds.size,
          friend_sources: { client: clientCount, webapi: webCount },
        });
      } else {
        log('debug', 'Friend/DB sync done (no changes)', {
          reason, friends_known: friendIds.size, links_known: dbSteamIds.size,
          role_cleanup_skipped_active_friend: cleanupSkipped,
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
    setFriendCheckCacheStatus,
    getWebApiFriendCacheAgeMs,
    sendFriendRequest,
    removeFriendship,
    verifySteamLink,
    unverifySteamLink,
    requestProfileCardForSid,
    queueFriendRequestForId,
    resolveManualFriendRequest,
    collectKnownFriendIds,
    resolveCachedPersonaName,
    fetchPersonaNames,
    syncFriendsAndLinks,
  };
};
