'use strict';

module.exports = (context) => {
  Object.assign(globalThis, context);

// ---------- Steam Events ----------
function markLoggedOn(details) {
  runtimeState.logged_on = true;
  runtimeState.logging_in = false;
  state.loginInProgress = false;
  runtimeState.guard_required = null;
  state.pendingGuard = null;
  runtimeState.last_logged_on_at = nowSeconds();
  runtimeState.last_error = null;
  state.deadlockAppActive = false;
  state.deadlockGcReady = false;
  runtimeState.deadlock_gc_ready = false;
  state.deadlockGameRequestedAt = 0;
  state.lastGcHelloAttemptAt = 0;

  if (client.steamID && typeof client.steamID.getSteamID64 === 'function') {
    runtimeState.steam_id64 = client.steamID.getSteamID64();
  } else if (client.steamID) {
    runtimeState.steam_id64 = String(client.steamID);
  } else {
    runtimeState.steam_id64 = null;
  }

  try {
    client.setPersona(SteamUser.EPersonaState.Away);
  } catch (err) {
    log('warn', 'Failed to set persona away', { error: err.message });
  }
  ensureDeadlockGamePlaying(true);
  requestDeadlockGcTokens('post-login');

  log('info', 'Steam login successful', {
    country: details ? details.publicIPCountry : undefined,
    cellId: details ? details.cellID : undefined,
    steam_id64: runtimeState.steam_id64,
  });

  // Sync friends <-> DB shortly after login (wait a bit for friend list to load)
  setTimeout(() => {
    syncFriendsAndLinks('post-login').catch((err) => {
      log('warn', 'Friend sync after login failed', { error: err && err.message ? err.message : String(err) });
    });
  }, 5000);

  scheduleStatePublish({ reason: 'logged_on' });
}

client.on('loggedOn', (details) => {
  deadlockGcBot.refreshGameVersion().catch((err) => {
    log('warn', 'Auto-update of game version failed', { error: err.message });
  });
  markLoggedOn(details);
});
client.on('webSession', () => { log('debug', 'Steam web session established'); });
client.on('steamGuard', (domain, callback, lastCodeWrong) => {
  state.pendingGuard = { domain, callback };
  const norm = String(domain || '').toLowerCase();

  // Detect guard type based on domain
  // Email: contains "email", "@", or is a domain like "steam.earlysalty.com"
  const isEmail = norm.includes('email') || norm.includes('@') || (norm.includes('.') && !norm.includes('two-factor') && !norm.includes('authenticator') && !norm.includes('mobile') && !norm.includes('device'));

  runtimeState.guard_required = {
    domain: domain || null,
    type: isEmail ? 'email' : (norm.includes('two-factor') || norm.includes('authenticator') || norm.includes('mobile')) ? 'totp' : (norm.includes('device') ? 'device' : 'unknown'),
    last_code_wrong: Boolean(lastCodeWrong),
    requested_at: nowSeconds(),
  };
  runtimeState.logging_in = true;
  log('info', 'Steam Guard challenge received', { domain: domain || null, lastCodeWrong: Boolean(lastCodeWrong) });
  scheduleStatePublish({ reason: 'steam_guard', domain: domain || null, last_code_wrong: Boolean(lastCodeWrong) });
});
client.on('refreshToken', (token) => {
  updateRefreshToken(token);
  const storage = writeToken(REFRESH_TOKEN_PATH, state.tokens.refreshToken, STEAM_VAULT_REFRESH_TOKEN);
  log('info', 'Stored refresh token', { storage });
});
client.on('machineAuthToken', (token) => {
  updateMachineToken(token);
  const storage = writeToken(MACHINE_TOKEN_PATH, state.tokens.machineAuthToken, STEAM_VAULT_MACHINE_TOKEN);
  log('info', 'Stored machine auth token', { storage });
});
client.on('_gcTokens', () => {
  const count = getDeadlockGcTokenCount();
  const delta = count - state.lastLoggedGcTokenCount;
  state.lastLoggedGcTokenCount = count;
  log('info', 'Received GC tokens update', {
    count,
    delta,
  });
  writeDeadlockGcTrace('gc_tokens_update', {
    count,
    delta,
  });
  deadlockGcBot.cachedHello = null;
  deadlockGcBot.cachedLegacyHello = null;
  if (state.deadlockAppActive && !state.deadlockGcReady) {
    log('debug', 'Retrying GC hello after token update');
    sendDeadlockGcHello(true);
  }
});

client.on('appLaunched', (appId) => {
  log('info', 'Steam app launched', { appId });
  if (Number(appId) !== Number(DEADLOCK_APP_ID)) return;
  
  log('info', 'Deadlock app launched - GC session starting');
  state.deadlockAppActive = true;
  state.deadlockGcReady = false;
  runtimeState.deadlock_gc_ready = false;
  requestDeadlockGcTokens('app_launch');
  
  // Wait a bit longer for GC to initialize
  setTimeout(() => {
    log('debug', 'Sending GC hello after app launch');
    sendDeadlockGcHello(true);
  }, 4000); // Increased delay
});
client.on('appQuit', (appId) => {
  log('info', 'Steam app quit', { appId });
  if (Number(appId) !== Number(DEADLOCK_APP_ID)) return;

  log('info', 'Deadlock app quit – GC session ended');
  state.deadlockAppActive = false;
  state.deadlockGcReady = false;
  runtimeState.deadlock_gc_ready = false;
  flushDeadlockGcWaiters(new Error('Deadlock app quit'));
  flushPendingPlaytestInvites(new Error('Deadlock app quit'));
  gcProfileCard.flushPending(new Error('Deadlock app quit'));
});

// Track friend relationship changes to auto-save steam links to DB
client.on('friendRelationship', (steamId, relationship) => {
  const sid64Raw = steamId && typeof steamId.getSteamID64 === 'function' ? steamId.getSteamID64() : String(steamId || '');
  const sid64 = normalizeSteamId64(sid64Raw);
  const relName = relationshipName(relationship);
  const EFriendRelationship = SteamUser.EFriendRelationship || {};

  log('info', 'Friend relationship changed', {
    steam_id64: sid64 || sid64Raw || null,
    relationship: relationship,
    relationship_name: relName,
  });

  if (sid64 && typeof setFriendCheckCacheStatus === 'function') {
    const isFriend = Number(relationship) === Number(EFriendRelationship.Friend);
    setFriendCheckCacheStatus(sid64, isFriend, 'friendRelationship_event');
  }

  if (Number(relationship) === Number(EFriendRelationship.RequestRecipient)) {
    if (!sid64) {
      log('warn', 'Ignoring incoming friend request with invalid SteamID', { steam_id64: sid64Raw || null });
      return;
    }
    if (!selectPendingFriendRequestForSteamIdStmt || typeof selectPendingFriendRequestForSteamIdStmt.get !== 'function') {
      log('warn', 'Incoming friend request whitelist check unavailable - refusing auto-accept', { steam_id64: sid64 });
      return;
    }

    let pendingRequest = null;
    try {
      pendingRequest = selectPendingFriendRequestForSteamIdStmt.get(sid64);
    } catch (err) {
      log('warn', 'Failed to verify incoming friend request against DB', {
        steam_id64: sid64,
        error: err && err.message ? err.message : String(err),
      });
      return;
    }
    if (!pendingRequest) {
      log('warn', 'Ignoring unsolicited incoming friend request', { steam_id64: sid64 });
      return;
    }

    log('info', 'Accepting incoming friend request (matched pending queue row)', {
      steam_id64: sid64,
      requested_at: pendingRequest.requested_at || null,
      attempts: pendingRequest.attempts || 0,
    });
    try {
      client.addFriend(steamId, (err) => {
        if (err) {
          log('warn', 'Failed to accept incoming friend request', {
            steam_id64: sid64,
            error: err && err.message ? err.message : String(err),
          });
        } else {
          log('info', 'Successfully accepted friend request', { steam_id64: sid64 });
          if (typeof resolveManualFriendRequest === 'function') {
            const resolved = resolveManualFriendRequest(sid64, 'incoming_accept');
            if (!resolved) {
              log('warn', 'Incoming friend request accepted but queue row cleanup failed', {
                steam_id64: sid64,
              });
            }
          }
        }
      });
    } catch (err) {
      log('error', 'Error in addFriend for incoming request', {
        steam_id64: sid64,
        error: err && err.message ? err.message : String(err),
      });
    }
    return;
  }

  if (Number(relationship) === Number(EFriendRelationship.Friend)) {
    log('info', 'New friend confirmed, checking steam_links', { steam_id64: sid64 || sid64Raw });

    const cachedName = resolveCachedPersonaName(sid64);
    if (verifySteamLink(sid64, cachedName)) {
      log('info', 'Verified existing steam_link', {
        steam_id64: sid64,
        name: cachedName || undefined,
      });
    }

    if (!cachedName) {
      fetchPersonaNames([sid64]).then((map) => {
        const normalized = normalizeSteamId64(sid64);
        const fetchedName = normalized ? map.get(normalized) : null;
        if (fetchedName) verifySteamLink(sid64, fetchedName);
      }).catch((err) => {
        log('debug', 'Failed to fetch persona for new friend', {
          steam_id64: sid64,
          error: err && err.message ? err.message : String(err),
        });
      });
    }

    // Direkt nach neuer Freundschaft: PlayerCard nur für diesen Account laden (Rate-Limit: 10 Min)
    requestProfileCardForSid(sid64);
    return;
  }

  if (Number(relationship) === Number(EFriendRelationship.None)) {
    log('info', 'Friendship ended, unverifying in steam_links', { steam_id64: sid64 || sid64Raw });
    if (unverifySteamLink(sid64 || sid64Raw)) {
      log('info', 'Unverified steam link', { steam_id64: sid64 || sid64Raw });
    }
  }
  });

client.on('receivedFromGC', (appId, msgType, payload) => {
  const messageId = msgType & ~PROTO_MASK;
  const payloadHex = payload ? payload.toString('hex').substring(0, 100) : 'none';
  const isDeadlockApp = DEADLOCK_APP_IDS.includes(Number(appId));

  writeDeadlockGcTrace('gc_message', {
    appId,
    msgType,
    messageId,
    payloadHex,
    isDeadlockApp,
  });

  // ENHANCED DEBUG: Log ALL GC messages for diagnosis
  log('info', '🚀 GC MESSAGE RECEIVED', {
    appId,
    messageId,
    messageIdHex: messageId.toString(16),
    msgType,
    msgTypeHex: msgType.toString(16),
    payloadLength: payload ? payload.length : 0,
    payloadHex,
    isDeadlockApp,
    expectedWelcome: GC_MSG_CLIENT_WELCOME,
    expectedResponses: playtestMsgConfigs.map(p => p.response)
  });

  if (messageId === GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD_RESPONSE && state.heroBuildPublishWaiter) {
    loadHeroBuildProto()
      .then(() => {
        const resp = getUpdateHeroBuildResponseMsg().decode(payload);
        state.heroBuildPublishWaiter.resolve(resp);
      })
      .catch((err) => state.heroBuildPublishWaiter.reject(err));
    return;
  }

  // Handle GC Build Search responses
  if (messageId === GC_MSG_FIND_HERO_BUILDS_RESPONSE && isDeadlockApp) {
    if (gcBuildSearch.handleGcMessage(appId, msgType, payload)) {
      return;
    }
  }

  // Fallback path: some GC responses may not be routed via steam-user job callbacks.
  if (isDeadlockApp && gcProfileCard.handleGcMessage(appId, msgType, payload)) {
    return;
  }

  if ((messageId === GC_MSG_CLIENT_WELCOME || messageId === 9019) && isDeadlockApp) {
    log('info', '?? RECEIVED DEADLOCK GC WELCOME - GC CONNECTION ESTABLISHED!', {
      appId,
      messageId,
      payloadLength: payload ? payload.length : 0
    });
    notifyDeadlockGcReady();
    return;
  }

  const matchingResponse = playtestMsgConfigs.find(config => config.response === messageId);
  if (matchingResponse || messageId === state.playtestIds.response) {
    log('info', '?? POTENTIAL PLAYTEST RESPONSE DETECTED!', {
      appId,
      messageId,
      configName: matchingResponse?.name || 'direct_match',
      sendId: matchingResponse?.send ?? state.playtestIds.send,
      responseId: matchingResponse?.response ?? state.playtestIds.response
    });
    handlePlaytestInviteResponse(appId, msgType, payload);
    return;
  }

  if (!isDeadlockApp) return;

  log('debug', 'Received unknown GC message', {
    msgType: messageId,
    expectedWelcome: GC_MSG_CLIENT_WELCOME,
    expectedPlaytestResponse: state.playtestIds.response
  });
});
client.on('disconnected', (eresult, msg) => {
  runtimeState.logged_on = false;
  runtimeState.logging_in = false;
  state.loginInProgress = false;
  runtimeState.last_disconnect_at = nowSeconds();
  runtimeState.last_disconnect_eresult = eresult;
  state.deadlockAppActive = false;
  state.deadlockGcReady = false;
  runtimeState.deadlock_gc_ready = false;
  state.lastLoggedGcTokenCount = 0;
  flushDeadlockGcWaiters(new Error('Steam disconnected'));
  flushPendingPlaytestInvites(new Error('Steam disconnected'));
  gcProfileCard.flushPending(new Error('Steam disconnected'));
  log('warn', 'Steam disconnected', { eresult, msg });
  scheduleReconnect('disconnect');
  scheduleStatePublish({ reason: 'disconnected', eresult });
});
client.on('error', (err) => {
  runtimeState.last_error = { message: err && err.message ? err.message : String(err), eresult: err && typeof err.eresult === 'number' ? err.eresult : undefined };
  runtimeState.logging_in = false; state.loginInProgress = false;
  const text = String(err && err.message ? err.message : '').toLowerCase();
  log('error', 'Steam client error', { error: runtimeState.last_error.message, eresult: runtimeState.last_error.eresult });
  if (text.includes('invalid refresh') || text.includes('expired') || text.includes('refresh token')) {
    if (state.tokens.refreshToken) {
      log('warn', 'Clearing refresh token after authentication failure');
      updateRefreshToken('');
      writeToken(REFRESH_TOKEN_PATH, '', STEAM_VAULT_REFRESH_TOKEN);
    }
    return;
  }
  if (text.includes('ratelimit') || text.includes('rate limit') || text.includes('throttle')) {
    log('warn', 'Rate limit encountered; waiting for explicit login task');
    return;
  }
  scheduleReconnect('error');
  scheduleStatePublish({ reason: 'error', message: runtimeState.last_error ? runtimeState.last_error.message : null });
});
client.on('sessionExpired', () => {
  log('warn', 'Steam session expired');
  runtimeState.logged_on = false;
  scheduleReconnect('session-expired');
  scheduleStatePublish({ reason: 'session_expired' });
});


  return {};
};
