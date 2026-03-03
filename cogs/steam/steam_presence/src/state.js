'use strict';

/**
 * State — standalone state publishing, task completion
 * Context: { state, runtimeState, log, nowSeconds, COMMAND_BOT_KEY,
 *            upsertStandaloneStateStmt, steamTaskCountsStmt, steamTaskRecentStmt,
 *            finishTaskStmt }
 */
module.exports = (ctx) => {
  const {
    state, runtimeState, log, nowSeconds, COMMAND_BOT_KEY,
    upsertStandaloneStateStmt, steamTaskCountsStmt, steamTaskRecentStmt,
    finishTaskStmt,
  } = ctx;

  function safeJsonStringify(value) {
    try { return JSON.stringify(value); }
    catch (err) { log('warn', 'Failed to stringify JSON', { error: err.message }); return null; }
  }

  function safeJsonParse(value) {
    if (!value) return {};
    try { return JSON.parse(value); }
    catch (err) { throw new Error(`Invalid JSON payload: ${err.message}`); }
  }

  function safeNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
  }

  function wrapOk(result) {
    if (result && typeof result === 'object' && Object.prototype.hasOwnProperty.call(result, 'ok')) {
      return result;
    }
    if (result === undefined) return { ok: true };
    return { ok: true, data: result };
  }

  function truncateError(msg, maxLen = 500) {
    if (!msg) return null;
    const s = String(msg);
    return s.length > maxLen ? s.slice(0, maxLen) + '…' : s;
  }

  function getStatusPayload() {
    return {
      account_name: runtimeState.account_name,
      account_password_configured: Boolean(runtimeState.account_password_configured),
      logged_on: runtimeState.logged_on,
      logging_in: runtimeState.logging_in,
      steam_id64: runtimeState.steam_id64,
      refresh_token_present: runtimeState.refresh_token_present,
      machine_token_present: runtimeState.machine_token_present,
      guard_required: runtimeState.guard_required,
      last_error: runtimeState.last_error,
      last_login_attempt_at: runtimeState.last_login_attempt_at,
      last_login_source: runtimeState.last_login_source,
      last_logged_on_at: runtimeState.last_logged_on_at,
      last_disconnect_at: runtimeState.last_disconnect_at,
      last_disconnect_eresult: runtimeState.last_disconnect_eresult,
      last_guard_submission_at: runtimeState.last_guard_submission_at,
      deadlock_gc_ready: runtimeState.deadlock_gc_ready,
    };
  }

  function buildStandaloneSnapshot() {
    const snapshot = {
      timestamp: new Date().toISOString(),
      runtime: getStatusPayload(),
      tasks: { counts: {}, recent: [] },
    };

    try {
      const rows = steamTaskCountsStmt.all();
      const counts = {};
      for (const row of rows) {
        const status = row && row.status ? String(row.status).toUpperCase() : 'UNKNOWN';
        const count = Number(row && row.count ? row.count : 0) || 0;
        counts[status] = count;
      }
      snapshot.tasks.counts = counts;
    } catch (err) {
      log('warn', 'Failed to collect steam task counts', { error: err.message });
    }

    try {
      const rows = steamTaskRecentStmt.all();
      snapshot.tasks.recent = rows.map((row) => ({
        id: Number(row.id),
        type: row.type,
        status: row.status,
        updated_at: row.updated_at,
        finished_at: row.finished_at,
      }));
    } catch (err) {
      log('warn', 'Failed to collect recent steam tasks', { error: err.message });
    }

    return snapshot;
  }

  function publishStandaloneState(context) {
    try {
      const snapshot = buildStandaloneSnapshot();
      if (context) snapshot.context = context;
      const payloadJson = safeJsonStringify(snapshot) || '{}';
      upsertStandaloneStateStmt.run({
        bot: COMMAND_BOT_KEY,
        heartbeat: nowSeconds(),
        payload: payloadJson,
      });
    } catch (err) {
      log('warn', 'Failed to publish standalone state', { error: err.message });
    }
  }

  function scheduleStatePublish(context) {
    try { publishStandaloneState(context); }
    catch (err) { log('warn', 'State publish failed', { error: err.message }); }
  }

  function completeTask(id, status, result = undefined, error = undefined) {
    const finishedAt = nowSeconds();
    const resultJson = result === undefined ? null : safeJsonStringify(result);
    const errorText = error ? String(error) : null;
    finishTaskStmt.run(status, resultJson, errorText, finishedAt, finishedAt, id);
    scheduleStatePublish({ reason: 'task', status, task_id: id });
  }

  return {
    safeJsonStringify,
    safeJsonParse,
    safeNumber,
    wrapOk,
    truncateError,
    getStatusPayload,
    buildStandaloneSnapshot,
    publishStandaloneState,
    scheduleStatePublish,
    completeTask,
  };
};
