'use strict';

/**
 * Playtest — Deadlock GC playtest invite management
 * Context: { state, client, log, writeDeadlockGcTrace, deadlockGcBot,
 *            getWorkingAppId, normalizeToBuffer, PROTO_MASK,
 *            playtestMsgConfigs, DEFAULT_PLAYTEST_MSG_IDS, buildPlaytestPayloadOverrideFn,
 *            waitForDeadlockGcReady,
 *            MIN_PLAYTEST_INVITE_TIMEOUT_MS, DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS,
 *            DEFAULT_PLAYTEST_INVITE_ATTEMPTS, PLAYTEST_RETRY_DELAY_MS,
 *            INVITE_RESPONSE_MIN_TIMEOUT_MS, DEFAULT_GC_READY_TIMEOUT_MS,
 *            DEFAULT_GC_READY_ATTEMPTS, MIN_GC_READY_TIMEOUT_MS,
 *            sleep, normalizeTimeoutMs, normalizeAttempts, isTimeoutError }
 */
module.exports = (ctx) => {
  const {
    state, client, log, writeDeadlockGcTrace, deadlockGcBot,
    getWorkingAppId, normalizeToBuffer, PROTO_MASK,
    playtestMsgConfigs, buildPlaytestPayloadOverrideFn,
    MIN_PLAYTEST_INVITE_TIMEOUT_MS, DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS,
    DEFAULT_PLAYTEST_INVITE_ATTEMPTS, PLAYTEST_RETRY_DELAY_MS,
    INVITE_RESPONSE_MIN_TIMEOUT_MS, DEFAULT_GC_READY_TIMEOUT_MS,
    DEFAULT_GC_READY_ATTEMPTS, MIN_GC_READY_TIMEOUT_MS,
    sleep, normalizeTimeoutMs, normalizeAttempts, isTimeoutError,
    DEADLOCK_APP_ID,
  } = ctx;

  const PLAYTEST_RESPONSE_MAP = {
    0: { key: 'eResponse_Success', message: 'Einladung erfolgreich übermittelt.' },
    1: { key: 'eResponse_InternalError', message: 'Interner Fehler bei Steam. Bitte versuche es in ein paar Minuten erneut.' },
    3: { key: 'eResponse_InvalidFriend', message: 'Wir sind keine Steam-Freunde. Bitte nimm die Freundschaftsanfrage des Bots auf Steam an.' },
    4: { key: 'eResponse_NotFriendsLongEnough', message: 'Steam-Beschränkung: Die Freundschaft muss mind. 30 Tage bestehen.' },
    5: { key: 'eResponse_AlreadyHasGame', message: 'Du hast Deadlock bereits! Prüfe deine Steam-Bibliothek oder https://store.steampowered.com/account/playtestinvites' },
    6: { key: 'eResponse_LimitedUser', message: 'Dein Steam-Account ist eingeschränkt (Limited User). Du musst mind. 5$ auf Steam ausgegeben haben, um Invites zu erhalten.' },
    7: { key: 'eResponse_InviteLimitReached', message: 'Das tägliche Invite-Limit ist erreicht. Bitte versuche es morgen erneut.' },
  };

  // Mutable state for working message IDs (shared via state object)
  // state.playtestIds.send / state.playtestIds.response

  function formatPlaytestError(response) {
    if (!response || typeof response !== 'object') return null;
    const message = response.message ? String(response.message).trim() : '';
    const codeRaw = Object.prototype.hasOwnProperty.call(response, 'code') ? response.code : null;
    let codeDisplay = null;
    if (codeRaw !== null && codeRaw !== undefined) {
      const maybeNumber = Number(codeRaw);
      if (Number.isFinite(maybeNumber)) codeDisplay = `Code ${maybeNumber}`;
      else if (typeof codeRaw === 'string' && codeRaw.trim()) codeDisplay = `Code ${codeRaw.trim()}`;
    }
    const key = response.key ? String(response.key).trim() : '';
    const meta = [];
    if (codeDisplay) meta.push(codeDisplay);
    if (key) meta.push(key);
    const parts = [];
    if (message) parts.push(message);
    if (meta.length) parts.push(`(${meta.join(' / ')})`);
    return parts.join(' ').trim() || null;
  }

  function encodeSubmitPlaytestUserPayload(accountId, location) {
    return deadlockGcBot.encodePlaytestInvitePayload(accountId, location);
  }

  function removePendingPlaytestInvite(entry) {
    const idx = state.pendingPlaytestInviteResponses.indexOf(entry);
    if (idx >= 0) state.pendingPlaytestInviteResponses.splice(idx, 1);
  }

  function flushPendingPlaytestInvites(error) {
    while (state.pendingPlaytestInviteResponses.length) {
      const entry = state.pendingPlaytestInviteResponses.shift();
      if (!entry) continue;
      if (entry.timer) clearTimeout(entry.timer);
      try { entry.reject(error || new Error('GC-Verbindung getrennt')); } catch (_) {}
    }
  }

  function sendPlaytestInviteOnce(accountId, location, timeoutMs) {
    const effectiveTimeout = Math.max(
      INVITE_RESPONSE_MIN_TIMEOUT_MS,
      Number.isFinite(timeoutMs) ? Number(timeoutMs) : DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS
    );
    const estimatedPayloadVariants = buildPlaytestPayloadOverrideFn ? 1 : 6;

    return new Promise((resolve, reject) => {
      const entry = {
        resolve: null, reject: null, timer: null, attemptTimers: [],
        attempts: 0,
        maxAttempts: Math.max(1, (playtestMsgConfigs.length || 1)) * estimatedPayloadVariants,
      };

      const cleanup = () => {
        if (entry.timer) clearTimeout(entry.timer);
        if (entry.attemptTimers) { entry.attemptTimers.forEach((t) => clearTimeout(t)); entry.attemptTimers = []; }
        removePendingPlaytestInvite(entry);
      };

      entry.resolve = (value) => { cleanup(); resolve(value); };
      entry.reject = (err) => { cleanup(); reject(err); };

      entry.timer = setTimeout(
        () => entry.reject(new Error('Timeout beim Warten auf GC-Antwort')),
        effectiveTimeout
      );

      state.pendingPlaytestInviteResponses.push(entry);

      const payloadVersions = buildPlaytestPayloadOverrideFn ? ['override'] : ['native'];
      let attemptCount = 0;
      const messageConfigs = playtestMsgConfigs.length ? playtestMsgConfigs : [];

      for (const msgConfig of messageConfigs) {
        for (const payloadVersion of payloadVersions) {
          const t = setTimeout(() => {
            try {
              const context = { accountId, location, payloadVersion, attempt: attemptCount, message: msgConfig };
              const payloadRaw = buildPlaytestPayloadOverrideFn
                ? buildPlaytestPayloadOverrideFn(context)
                : encodeSubmitPlaytestUserPayload(accountId, location);
              const payload = buildPlaytestPayloadOverrideFn ? normalizeToBuffer(payloadRaw) : payloadRaw;

              if (!payload || !payload.length) throw new Error('Playtest payload is empty');

              const targetAppId = Number.isFinite(msgConfig.appId) ? Number(msgConfig.appId) : getWorkingAppId();
              client.sendToGC(targetAppId, PROTO_MASK + msgConfig.send, {}, payload);
              writeDeadlockGcTrace('send_playtest_invite', {
                accountId, location, appId: targetAppId, messageId: msgConfig.send,
                payloadVersion, overridePayload: Boolean(buildPlaytestPayloadOverrideFn),
                payloadHex: payload.toString('hex').substring(0, 200),
              });
              log('info', 'Deadlock playtest invite requested', {
                accountId, location, messageId: msgConfig.send, messageName: msgConfig.name,
                payloadVersion, appId: targetAppId, payloadLength: payload.length,
                payloadHex: payload.toString('hex').substring(0, 50),
                overridePayload: Boolean(buildPlaytestPayloadOverrideFn),
              });

              if (attemptCount === 0) {
                state.playtestIds.send = msgConfig.send;
                state.playtestIds.response = msgConfig.response;
              }
            } catch (err) {
              log('warn', 'Failed to send playtest invite attempt', {
                error: err.message, messageId: msgConfig.send, payloadVersion,
                overridePayload: Boolean(buildPlaytestPayloadOverrideFn),
              });
              writeDeadlockGcTrace('playtest_send_error', {
                error: err && err.message ? err.message : err, messageId: msgConfig.send, payloadVersion,
              });
            }
          }, attemptCount * 200);
          entry.attemptTimers.push(t);
          attemptCount++;
        }
      }

      if (!buildPlaytestPayloadOverrideFn && DEADLOCK_APP_ID !== 1422450) {
        const t = setTimeout(() => {
          try {
            const payload = encodeSubmitPlaytestUserPayload(accountId, location);
            client.sendToGC(1422450, PROTO_MASK + state.playtestIds.send, {}, payload);
            log('info', 'Fallback invite attempt to original Deadlock app', { accountId, location });
          } catch (err) {
            log('warn', 'Fallback attempt failed', { error: err.message });
          }
        }, attemptCount * 200);
        entry.attemptTimers.push(t);
      }
    });
  }

  async function sendPlaytestInvite(accountId, location, timeoutMs = DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS, options = {}) {
    const inviteTimeout = normalizeTimeoutMs(timeoutMs, DEFAULT_PLAYTEST_INVITE_TIMEOUT_MS, MIN_PLAYTEST_INVITE_TIMEOUT_MS);
    const inviteAttempts = normalizeAttempts(
      Object.prototype.hasOwnProperty.call(options, 'retryAttempts') ? options.retryAttempts : undefined,
      DEFAULT_PLAYTEST_INVITE_ATTEMPTS, 5
    );
    const gcAttempts = normalizeAttempts(
      Object.prototype.hasOwnProperty.call(options, 'gcRetryAttempts') ? options.gcRetryAttempts : undefined,
      DEFAULT_GC_READY_ATTEMPTS, 5
    );
    const gcTimeoutOverride = Object.prototype.hasOwnProperty.call(options, 'gcTimeoutMs')
      ? options.gcTimeoutMs
      : (Object.prototype.hasOwnProperty.call(options, 'gc_ready_timeout_ms') ? options.gc_ready_timeout_ms : options.gcTimeout);
    const gcTimeout = normalizeTimeoutMs(
      gcTimeoutOverride !== undefined ? gcTimeoutOverride : Math.max(inviteTimeout, DEFAULT_GC_READY_TIMEOUT_MS),
      Math.max(inviteTimeout, DEFAULT_GC_READY_TIMEOUT_MS),
      MIN_GC_READY_TIMEOUT_MS
    );
    let attempt = 0;
    let lastError = null;

    log('info', 'Deadlock playtest invite timings', { inviteTimeoutMs: inviteTimeout, inviteAttempts, gcTimeoutMs: gcTimeout, gcAttempts });

    while (attempt < inviteAttempts) {
      attempt += 1;
      try {
        // waitForDeadlockGcReady is late-bound
        await ctx.waitForDeadlockGcReady(gcTimeout, { retryAttempts: gcAttempts });
        return await sendPlaytestInviteOnce(accountId, location, inviteTimeout);
      } catch (err) {
        lastError = err;
        if (attempt >= inviteAttempts || !isTimeoutError(err)) break;
        log('warn', 'Deadlock playtest invite timed out - retrying', { attempt, attempts: inviteAttempts, timeoutMs: inviteTimeout });
        await sleep(PLAYTEST_RETRY_DELAY_MS);
      }
    }

    if (lastError && typeof lastError === 'object') {
      lastError.timeoutMs = inviteTimeout;
      lastError.gcTimeoutMs = gcTimeout;
      lastError.attempts = attempt;
    }
    throw lastError || new Error('Playtest invite failed');
  }

  function handlePlaytestInviteResponse(appid, msgType, buffer) {
    const safeMsgType = Number.isFinite(msgType) ? Number(msgType) : 0;
    const messageId = safeMsgType & ~PROTO_MASK;
    const payloadBuffer = Buffer.isBuffer(buffer) ? buffer : normalizeToBuffer(buffer);

    log('info', 'Received GC playtest response', {
      appId: appid, messageId,
      bufferLength: payloadBuffer ? payloadBuffer.length : 0,
      bufferHex: payloadBuffer ? payloadBuffer.toString('hex').substring(0, 100) : 'none',
    });
    writeDeadlockGcTrace('received_playtest_response', {
      appId: appid, messageId,
      payloadHex: payloadBuffer ? payloadBuffer.toString('hex').substring(0, 200) : 'none',
    });

    if (!payloadBuffer || !payloadBuffer.length) {
      log('warn', 'Received empty playtest response payload', { appId: appid, messageId });
      return;
    }
    if (!state.pendingPlaytestInviteResponses.length) {
      log('warn', 'Received unexpected playtest invite response', { appId: appid, messageId });
      return;
    }

    const matchingConfig = playtestMsgConfigs.find(config => config.response === messageId);
    if (matchingConfig) {
      log('info', 'SUCCESS: Found working message ID pair!', {
        sendId: matchingConfig.send, responseId: matchingConfig.response,
        configName: matchingConfig.name, appId: appid,
      });
      state.playtestIds.send = matchingConfig.send;
      state.playtestIds.response = matchingConfig.response;
    }

    const entry = state.pendingPlaytestInviteResponses.shift();
    if (entry && entry.timer) clearTimeout(entry.timer);

    const parsedResponse = deadlockGcBot.decodePlaytestInviteResponse(payloadBuffer);
    log('info', 'DEBUG: Decoded playtest response', { parsed: JSON.stringify(parsedResponse) });
    const code = parsedResponse && typeof parsedResponse.code === 'number' ? parsedResponse.code : null;
    const mapping = Object.prototype.hasOwnProperty.call(PLAYTEST_RESPONSE_MAP, code || 0)
      ? PLAYTEST_RESPONSE_MAP[code || 0]
      : { key: 'unknown', message: 'Unbekannte Antwort des Game Coordinators.' };

    const response = {
      success: parsedResponse ? Boolean(parsedResponse.success) : code === 0,
      code: code === null ? null : Number(code),
      key: mapping.key,
      message: mapping.message,
      messageId,
      appId: appid,
      workingConfig: matchingConfig?.name || 'unknown',
    };

    log('info', 'Playtest invite response decoded', { success: response.success, code: response.code, key: response.key, message: response.message });

    if (entry && entry.resolve) {
      try { entry.resolve({ success: response.success, response }); return; }
      catch (err) { log('warn', 'Failed to resolve playtest invite promise', { error: err.message }); }
    }
    log('warn', 'No pending playtest promise to resolve');
  }

  return {
    formatPlaytestError,
    encodeSubmitPlaytestUserPayload,
    removePendingPlaytestInvite,
    flushPendingPlaytestInvites,
    sendPlaytestInvite,
    sendPlaytestInviteOnce,
    handlePlaytestInviteResponse,
  };
};
