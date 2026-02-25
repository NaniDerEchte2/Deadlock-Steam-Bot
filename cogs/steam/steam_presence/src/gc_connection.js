'use strict';

/**
 * GC Connection — Deadlock Game Coordinator session management
 * Context: { state, runtimeState, client, log, writeDeadlockGcTrace,
 *            deadlockGcBot, getWorkingAppId, normalizeToBuffer,
 *            getDeadlockGcTokenCount, requestDeadlockGcTokens,
 *            PROTO_MASK, GC_MSG_CLIENT_HELLO, GC_MSG_CLIENT_HELLO_ALT,
 *            DEADLOCK_GC_PROTOCOL_OVERRIDE_PATH, getHelloPayloadOverride,
 *            GC_CLIENT_HELLO_PROTOCOL_VERSION, RECONNECT_DELAY_MS,
 *            MIN_GC_READY_TIMEOUT_MS, DEFAULT_GC_READY_TIMEOUT_MS,
 *            DEFAULT_GC_READY_ATTEMPTS, GC_READY_RETRY_DELAY_MS,
 *            sleep, normalizeTimeoutMs, normalizeAttempts, isTimeoutError }
 * Note: ctx.initiateLogin is late-bound (set after auth.js is created)
 */
module.exports = (ctx) => {
  const {
    state, runtimeState, client, log, writeDeadlockGcTrace,
    deadlockGcBot, getWorkingAppId, normalizeToBuffer,
    getDeadlockGcTokenCount, requestDeadlockGcTokens,
    PROTO_MASK, GC_MSG_CLIENT_HELLO, GC_MSG_CLIENT_HELLO_ALT,
    DEADLOCK_GC_PROTOCOL_OVERRIDE_PATH, getHelloPayloadOverride,
    RECONNECT_DELAY_MS, MIN_GC_READY_TIMEOUT_MS, DEFAULT_GC_READY_TIMEOUT_MS,
    DEFAULT_GC_READY_ATTEMPTS, GC_READY_RETRY_DELAY_MS,
    sleep, normalizeTimeoutMs, normalizeAttempts, isTimeoutError,
    DEADLOCK_APP_ID,
  } = ctx;

  function clearReconnectTimer() {
    if (state.reconnectTimer) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
    }
  }

  function scheduleReconnect(reason, delayMs = RECONNECT_DELAY_MS) {
    if (!state.tokens.refreshToken || state.manualLogout || runtimeState.logged_on || state.loginInProgress || state.reconnectTimer) return;
    const delay = Math.max(1000, Number.isFinite(delayMs) ? delayMs : RECONNECT_DELAY_MS);
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      try {
        // ctx.initiateLogin is late-bound after auth.js is created
        const initiateLogin = ctx.initiateLogin;
        if (!initiateLogin) {
          log('warn', 'Auto reconnect: initiateLogin not yet available', { reason });
          return;
        }
        const result = initiateLogin('auto-reconnect', {});
        log('info', 'Auto reconnect attempt', { reason, result });
      } catch (err) {
        log('warn', 'Auto reconnect failed to start', { error: err.message, reason });
      }
    }, delay);
  }

  function ensureDeadlockGamePlaying(force = false) {
    const now = Date.now();
    if (!force && now - state.deadlockGameRequestedAt < 15000) {
      log('debug', 'Skipping gamesPlayed request - too recent', {
        timeSinceLastRequest: now - state.deadlockGameRequestedAt,
      });
      return;
    }

    try {
      const previouslyActive = state.deadlockAppActive;
      const appId = getWorkingAppId();

      if (!previouslyActive) {
        client.gamesPlayed([]);
        setTimeout(() => {
          client.gamesPlayed([appId]);
          log('info', 'Started playing Deadlock', { appId });
        }, 1000);
      } else {
        client.gamesPlayed([appId]);
      }

      state.deadlockGameRequestedAt = now;
      state.deadlockAppActive = true;

      log('info', 'Requested Deadlock GC session via gamesPlayed()', {
        appId, force, previouslyActive,
        steamId: client.steamID ? String(client.steamID) : 'not_logged_in',
      });
      requestDeadlockGcTokens('games_played');

      if (!previouslyActive) {
        state.deadlockGcReady = false;
        runtimeState.deadlock_gc_ready = false;
        setTimeout(() => {
          log('debug', 'Initiating GC handshake after game start');
          sendDeadlockGcHello(true);
        }, 3000);
      }
    } catch (err) {
      log('error', 'Failed to call gamesPlayed for Deadlock', {
        error: err.message,
        steamId: client.steamID ? String(client.steamID) : 'not_logged_in',
      });
    }
  }

  function sendDeadlockGcHello(force = false) {
    if (!state.deadlockAppActive) {
      log('debug', 'Skipping GC hello - app not active');
      return false;
    }

    const now = Date.now();
    if (!force && now - state.lastGcHelloAttemptAt < 2000) {
      log('debug', 'Skipping GC hello - too recent');
      return false;
    }

    const tokenCount = getDeadlockGcTokenCount();
    if (tokenCount <= 0) {
      log('warn', 'Sending GC hello without GC tokens', { tokenCount });
      requestDeadlockGcTokens('hello_no_tokens');
    } else if (tokenCount < 2) {
      requestDeadlockGcTokens('hello_low_tokens');
    }

    try {
      const payload = getDeadlockGcHelloPayload(force);
      const appId = getWorkingAppId();

      log('info', 'Sending Deadlock GC hello', {
        appId, payloadLength: payload.length, force,
        steamId: client.steamID ? String(client.steamID) : 'not_logged_in',
      });

      client.sendToGC(appId, PROTO_MASK + GC_MSG_CLIENT_HELLO, {}, payload);
      client.sendToGC(appId, PROTO_MASK + GC_MSG_CLIENT_HELLO_ALT, {}, payload);
      writeDeadlockGcTrace('send_gc_hello', {
        appId,
        payloadHex: payload.toString('hex').substring(0, 200),
        force, tokenCount,
      });
      state.lastGcHelloAttemptAt = now;

      setTimeout(() => {
        if (!state.deadlockGcReady) {
          log('warn', 'GC did not respond to hello within 5 seconds', {
            appId, timeSinceHello: Date.now() - now,
          });
          tryAlternativeGcHandshake();
        }
      }, 5000);

      return true;
    } catch (err) {
      log('error', 'Failed to send Deadlock GC hello', { error: err.message, stack: err.stack });
      return false;
    }
  }

  function tryAlternativeGcHandshake() {
    try {
      log('info', 'Attempting alternative GC handshake');
      deadlockGcBot.cachedHello = null;
      deadlockGcBot.cachedLegacyHello = null;
      const payload = getDeadlockGcHelloPayload(true);
      client.sendToGC(DEADLOCK_APP_ID, PROTO_MASK + GC_MSG_CLIENT_HELLO, {}, payload);
      log('debug', 'Sent refreshed GC hello payload');
    } catch (err) {
      log('error', 'Alternative GC handshake failed', { error: err.message });
    }
  }

  function removeGcWaiter(entry) {
    const idx = state.deadlockGcWaiters.indexOf(entry);
    if (idx >= 0) state.deadlockGcWaiters.splice(idx, 1);
  }

  function flushDeadlockGcWaiters(error) {
    while (state.deadlockGcWaiters.length) {
      const waiter = state.deadlockGcWaiters.shift();
      try {
        if (waiter) waiter.reject(error || new Error('Deadlock GC session reset'));
      } catch (_) {}
    }
  }

  function notifyDeadlockGcReady() {
    state.deadlockGcReady = true;
    runtimeState.deadlock_gc_ready = true;
    scheduleStatePublish({ reason: 'gc_ready' });
    writeDeadlockGcTrace('gc_ready', { waiters: state.deadlockGcWaiters.length });
    while (state.deadlockGcWaiters.length) {
      const waiter = state.deadlockGcWaiters.shift();
      try {
        if (waiter) waiter.resolve(true);
      } catch (_) {}
    }
  }

  function getDeadlockGcHelloPayload(force = false) {
    const overridePayload = getHelloPayloadOverride({ client, SteamUser: ctx.SteamUser });
    const normalizedOverride = normalizeToBuffer(overridePayload);
    if (normalizedOverride && normalizedOverride.length) {
      log('info', 'Using override Deadlock GC hello payload', { length: normalizedOverride.length });
      return Buffer.from(normalizedOverride);
    }
    if (overridePayload) {
      log('warn', 'Deadlock GC override hello payload invalid – falling back to auto builder', {
        path: DEADLOCK_GC_PROTOCOL_OVERRIDE_PATH,
      });
    }

    const payload = deadlockGcBot.getHelloPayload(force);
    if (!payload || !payload.length) {
      throw new Error('Unable to build Deadlock GC hello payload');
    }
    log('debug', 'Generated GC hello payload', {
      payloadLength: payload.length,
      payloadHex: payload.toString('hex'),
    });
    return payload;
  }

  function createDeadlockGcReadyPromise(timeout) {
    ensureDeadlockGamePlaying();
    requestDeadlockGcTokens('wait_gc_ready');
    if (state.deadlockGcReady) return Promise.resolve(true);

    const effectiveTimeout = Math.max(
      MIN_GC_READY_TIMEOUT_MS,
      Number.isFinite(timeout) ? Number(timeout) : DEFAULT_GC_READY_TIMEOUT_MS
    );

    return new Promise((resolve, reject) => {
      const entry = { resolve: null, reject: null, timer: null, interval: null, done: false };

      entry.resolve = (value) => {
        if (entry.done) return;
        entry.done = true;
        if (entry.timer) clearTimeout(entry.timer);
        if (entry.interval) clearInterval(entry.interval);
        removeGcWaiter(entry);
        resolve(value);
      };

      entry.reject = (err) => {
        if (entry.done) return;
        entry.done = true;
        if (entry.timer) clearTimeout(entry.timer);
        if (entry.interval) clearInterval(entry.interval);
        removeGcWaiter(entry);
        reject(err || new Error('Deadlock GC not ready'));
      };

      entry.timer = setTimeout(
        () => entry.reject(new Error('Timeout waiting for Deadlock GC')),
        effectiveTimeout
      );
      entry.interval = setInterval(() => {
        ensureDeadlockGamePlaying();
        sendDeadlockGcHello(false);
      }, 2000);

      state.deadlockGcWaiters.push(entry);
      sendDeadlockGcHello(true);
    });
  }

  async function waitForDeadlockGcReady(timeoutMs = DEFAULT_GC_READY_TIMEOUT_MS, options = {}) {
    const timeout = normalizeTimeoutMs(timeoutMs, DEFAULT_GC_READY_TIMEOUT_MS, MIN_GC_READY_TIMEOUT_MS);
    const attempts = normalizeAttempts(
      Object.prototype.hasOwnProperty.call(options, 'retryAttempts') ? options.retryAttempts : undefined,
      DEFAULT_GC_READY_ATTEMPTS, 5
    );
    let attempt = 0;
    let lastError = null;

    while (attempt < attempts) {
      attempt += 1;
      try {
        ensureDeadlockGamePlaying(true);
        await sleep(1000);
        await createDeadlockGcReadyPromise(timeout);
        log('info', 'Deadlock GC ready after attempt', { attempt, attempts });
        return true;
      } catch (err) {
        lastError = err || new Error('Deadlock GC not ready');
        state.deadlockGcReady = false;
        runtimeState.deadlock_gc_ready = false;

        log('warn', 'Deadlock GC attempt failed', {
          attempt, attempts, timeoutMs: timeout,
          error: err?.message || String(err),
          isTimeoutError: isTimeoutError(err),
        });

        if (attempt >= attempts || !isTimeoutError(err)) break;

        log('info', 'Retrying Deadlock GC handshake after delay', { attempt, attempts, delayMs: GC_READY_RETRY_DELAY_MS });

        state.deadlockAppActive = false;
        state.deadlockGcReady = false;
        runtimeState.deadlock_gc_ready = false;
        flushDeadlockGcWaiters(new Error('Retry attempt'));
        await sleep(GC_READY_RETRY_DELAY_MS);
      }
    }

    if (lastError && typeof lastError === 'object') {
      lastError.timeoutMs = timeout;
      lastError.attempts = attempt;
    }
    throw lastError;
  }

  // scheduleStatePublish is late-bound (set after state.js is created)
  function scheduleStatePublish(context) {
    if (ctx.scheduleStatePublish) {
      try { ctx.scheduleStatePublish(context); } catch (err) { log('warn', 'State publish failed', { error: err.message }); }
    }
  }

  return {
    clearReconnectTimer,
    scheduleReconnect,
    ensureDeadlockGamePlaying,
    sendDeadlockGcHello,
    tryAlternativeGcHandshake,
    removeGcWaiter,
    flushDeadlockGcWaiters,
    notifyDeadlockGcReady,
    getDeadlockGcHelloPayload,
    createDeadlockGcReadyPromise,
    waitForDeadlockGcReady,
  };
};
