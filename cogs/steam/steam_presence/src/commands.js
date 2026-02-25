'use strict';

module.exports = (context) => {
  Object.assign(globalThis, context);

// ---------- Standalone Command Handling ----------
let commandInProgress = false;

const COMMAND_HANDLERS = {
  status: () => ({ ok: true, data: getStatusPayload() }),
  login: (payload) => {
    const result = initiateLogin('command', payload || {});
    scheduleStatePublish({ reason: 'command-login' });
    return { ok: true, data: result };
  },
  logout: () => ({ ok: true, data: handleLogoutTask() }),
  'guard.submit': (payload) => ({ ok: true, data: handleGuardCodeTask(payload || {}) }),
  restart: () => {
    log('info', 'Restart command received - terminating process for restart');
    process.exit(0);
  },
};

function finalizeStandaloneCommand(commandId, status, resultObj, errorMessage) {
  const resultJson = resultObj === undefined ? null : safeJsonStringify(resultObj);
  const errorText = truncateError(errorMessage);
  try {
    finalizeCommandStmt.run(status, resultJson, errorText, commandId);
  } catch (err) {
    log('error', 'Failed to finalize standalone command', { error: err.message, command_id: commandId, status });
  }
}

function processNextCommand() {
  if (commandInProgress) return;

  let row;
  try {
    row = selectPendingCommandStmt.get(COMMAND_BOT_KEY);
  } catch (err) {
    log('error', 'Failed to fetch standalone command', { error: err.message });
    return;
  }

  if (!row) {
    return;
  }

  try {
    const claimed = markCommandRunningStmt.run(row.id);
    if (!claimed.changes) {
      setTimeout(processNextCommand, 0);
      return;
    }
  } catch (err) {
    log('error', 'Failed to mark standalone command running', { error: err.message, id: row.id });
    return;
  }

  commandInProgress = true;

  let payloadData = {};
  if (row.payload) {
    try {
      payloadData = safeJsonParse(row.payload);
    } catch (err) {
      log('warn', 'Invalid standalone command payload', { error: err.message, id: row.id });
      payloadData = {};
    }
  }

  const handler = COMMAND_HANDLERS[row.command];

  const finalize = (status, resultObj, errorMessage) => {
    finalizeStandaloneCommand(row.id, status, resultObj, errorMessage);
    try { publishStandaloneState({ reason: 'command', command: row.command, status }); }
    catch (err) { log('warn', 'Failed to publish state after command', { error: err.message, command: row.command }); }
    commandInProgress = false;
    setTimeout(processNextCommand, 0);
  };

  if (!handler) {
    finalize('error', { ok: false, error: 'unknown_command' }, `Unsupported command: ${row.command}`);
    return;
  }

  let outcome;
  try {
    outcome = handler(payloadData || {}, row);
  } catch (err) {
    const message = err && err.message ? err.message : String(err);
    finalize('error', { ok: false, error: message }, message);
    return;
  }

  if (outcome && typeof outcome.then === 'function') {
    outcome.then(
      (res) => finalize('success', wrapOk(res), null),
      (err) => {
        const message = err && err.message ? err.message : String(err);
        finalize('error', { ok: false, error: message }, message);
      },
    );
  } else {
    finalize('success', wrapOk(outcome), null);
  }
}

setInterval(() => {
  try { processNextCommand(); } catch (err) { log('error', 'Standalone command loop failed', { error: err.message }); }
}, Math.max(500, COMMAND_POLL_INTERVAL_MS));

processNextCommand();

setInterval(() => {
  try {
    if (runtimeState.logged_on && state.deadlockAppActive && !state.deadlockGcReady) {
      if (state.gcUnhealthySince === 0) {
        state.gcUnhealthySince = Date.now();
      } else if (Date.now() - state.gcUnhealthySince > GC_UNHEALTHY_THRESHOLD_MS) {
        log('error', 'Deadlock GC session has been unhealthy for too long. Suspecting game update required. Restarting service.', {
          unhealthyMs: Date.now() - state.gcUnhealthySince,
          thresholdMs: GC_UNHEALTHY_THRESHOLD_MS
        });
        shutdown(1);
      }
    } else {
      state.gcUnhealthySince = 0;
    }
    publishStandaloneState({ reason: 'heartbeat' });
  }
  catch (err) { log('warn', 'Standalone state heartbeat failed', { error: err.message }); }
}, Math.max(5000, STATE_PUBLISH_INTERVAL_MS));


  return {
    commandInProgress,
    COMMAND_HANDLERS,
  };
};
