'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const Database = require('better-sqlite3');

/**
 * Database — SQLite setup, schema, all prepared statements
 * Context: { log, DB_BUSY_TIMEOUT_MS, STEAM_TASKS_MAX_ROWS }
 */
module.exports = (ctx) => {
  const { log, DB_BUSY_TIMEOUT_MS, STEAM_TASKS_MAX_ROWS } = ctx;

  function resolveDbPath() {
    if (process.env.DEADLOCK_DB_PATH) return path.resolve(process.env.DEADLOCK_DB_PATH);
    const baseDir = process.env.DEADLOCK_DB_DIR
      ? path.resolve(process.env.DEADLOCK_DB_DIR)
      : path.join(os.homedir(), 'Documents', 'Deadlock', 'service');
    return path.join(baseDir, 'deadlock.sqlite3');
  }

  function ensureDir(dirPath) {
    try { fs.mkdirSync(dirPath, { recursive: true }); } catch (err) { if (err && err.code !== 'EEXIST') throw err; }
  }

  const dbPath = resolveDbPath();
  ensureDir(path.dirname(dbPath));
  log('info', 'Using SQLite database', { dbPath });

  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma(`busy_timeout = ${DB_BUSY_TIMEOUT_MS}`);

  // ---------- Steam Tasks ----------
  db.prepare(`
    CREATE TABLE IF NOT EXISTS steam_tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      payload TEXT,
      status TEXT NOT NULL DEFAULT 'PENDING',
      result TEXT,
      error TEXT,
      created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
      updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
      started_at INTEGER,
      finished_at INTEGER
    )
  `).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_steam_tasks_status ON steam_tasks(status, id)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_steam_tasks_updated ON steam_tasks(updated_at)`).run();

  // ---------- Steam Links + Friend Requests ----------
  db.prepare(`
    CREATE TABLE IF NOT EXISTS steam_links(
      user_id    INTEGER NOT NULL,
      steam_id   TEXT    NOT NULL,
      name       TEXT,
      verified   INTEGER DEFAULT 0,
      is_steam_friend INTEGER DEFAULT 0,
      primary_account INTEGER DEFAULT 0,
      deadlock_rank INTEGER,
      deadlock_rank_name TEXT,
      deadlock_subrank INTEGER,
      deadlock_badge_level INTEGER,
      deadlock_rank_updated_at INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY(user_id, steam_id)
    )
  `).run();
  for (const alterSql of [
    "ALTER TABLE steam_links ADD COLUMN is_steam_friend INTEGER DEFAULT 0",
    "ALTER TABLE steam_links ADD COLUMN deadlock_rank INTEGER",
    "ALTER TABLE steam_links ADD COLUMN deadlock_rank_name TEXT",
    "ALTER TABLE steam_links ADD COLUMN deadlock_subrank INTEGER",
    "ALTER TABLE steam_links ADD COLUMN deadlock_badge_level INTEGER",
    "ALTER TABLE steam_links ADD COLUMN deadlock_rank_updated_at INTEGER",
  ]) {
    try {
      db.prepare(alterSql).run();
    } catch (err) {
      const text = String((err && err.message) || '').toLowerCase();
      if (!text.includes('duplicate column name')) {
        log('warn', 'Failed to alter steam_links table', { alterSql, error: err && err.message ? err.message : String(err) });
      }
    }
  }
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_steam_links_user ON steam_links(user_id)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_steam_links_steam ON steam_links(steam_id)`).run();
  function ensureSteamLinkOwnershipGuardrails() {
    try {
      db.prepare(`
        CREATE UNIQUE INDEX IF NOT EXISTS uq_steam_links_steam_owner
        ON steam_links(steam_id)
        WHERE user_id != 0
      `).run();
    } catch (err) {
      log('warn', 'Failed to ensure unique steam_id ownership index (dirty historical data or SQLite limitation)', {
        error: err && err.message ? err.message : String(err),
      });
    }

    const guardStatements = [
      `DROP TRIGGER IF EXISTS trg_steam_links_owner_guard_insert`,
      `DROP TRIGGER IF EXISTS trg_steam_links_owner_guard_update`,
      `
        CREATE TRIGGER trg_steam_links_owner_guard_insert
        BEFORE INSERT ON steam_links
        FOR EACH ROW
        WHEN NEW.user_id != 0
          AND EXISTS (
            SELECT 1
              FROM steam_links
             WHERE steam_id = NEW.steam_id
               AND user_id NOT IN (NEW.user_id, 0)
          )
        BEGIN
          SELECT RAISE(ABORT, 'steam_links ownership conflict');
        END
      `,
      `
        CREATE TRIGGER trg_steam_links_owner_guard_update
        BEFORE UPDATE OF user_id, steam_id ON steam_links
        FOR EACH ROW
        WHEN NEW.user_id != 0
          AND (NEW.user_id != OLD.user_id OR NEW.steam_id != OLD.steam_id)
          AND EXISTS (
            SELECT 1
              FROM steam_links
             WHERE steam_id = NEW.steam_id
               AND user_id NOT IN (NEW.user_id, 0)
          )
        BEGIN
          SELECT RAISE(ABORT, 'steam_links ownership conflict');
        END
      `,
    ];
    for (const sql of guardStatements) {
      try {
        db.prepare(sql).run();
      } catch (err) {
        const statement = String(sql).trim().split('\n')[0] || '';
        log('warn', 'Failed to ensure steam_links ownership guardrail', {
          statement,
          error: err && err.message ? err.message : String(err),
        });
      }
    }
  }
  ensureSteamLinkOwnershipGuardrails();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS steam_friend_requests(
      steam_id TEXT PRIMARY KEY,
      status TEXT DEFAULT 'pending',
      requested_at INTEGER DEFAULT (strftime('%s','now')),
      last_attempt INTEGER,
      attempts INTEGER DEFAULT 0,
      error TEXT
    )
  `).run();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS steam_friend_check_cache(
      steam_id TEXT PRIMARY KEY,
      friend INTEGER NOT NULL,
      checked_at INTEGER NOT NULL
    )
  `).run();
  db.prepare(`
    CREATE TABLE IF NOT EXISTS steam_role_cleanup_pending(
      user_id INTEGER PRIMARY KEY,
      reason TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
      updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
    )
  `).run();
  db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_steam_role_cleanup_pending_updated
    ON steam_role_cleanup_pending(updated_at)
  `).run();

  // ---------- Standalone Dashboard Tables ----------
  db.prepare(`
    CREATE TABLE IF NOT EXISTS standalone_commands (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      bot TEXT NOT NULL,
      command TEXT NOT NULL,
      payload TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      result TEXT,
      error TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      started_at DATETIME,
      finished_at DATETIME
    )
  `).run();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS standalone_bot_state (
      bot TEXT PRIMARY KEY,
      heartbeat INTEGER NOT NULL,
      payload TEXT,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `).run();

  db.prepare(`CREATE INDEX IF NOT EXISTS idx_standalone_commands_status ON standalone_commands(bot, status, id)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_standalone_commands_created ON standalone_commands(created_at)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_standalone_state_updated ON standalone_bot_state(updated_at)`).run();

  // ---------- Cap Trigger + Prune ----------
  function installSteamTaskCapTrigger() {
    try {
      db.prepare(`
        CREATE TRIGGER IF NOT EXISTS trg_cap_steam_tasks
        AFTER INSERT ON steam_tasks
        BEGIN
          DELETE FROM steam_tasks
          WHERE id IN (
            SELECT id FROM steam_tasks
            WHERE status NOT IN ('PENDING','RUNNING')
            ORDER BY created_at ASC, id ASC
            LIMIT (
              SELECT CASE WHEN total > ${STEAM_TASKS_MAX_ROWS} THEN total - ${STEAM_TASKS_MAX_ROWS} ELSE 0 END
              FROM (SELECT COUNT(*) AS total FROM steam_tasks)
            )
          );
        END;
      `).run();
    } catch (err) {
      log('warn', 'Failed to ensure steam_tasks cap trigger', { error: err && err.message ? err.message : err });
    }
  }

  function pruneSteamTasks(reason = 'startup') {
    try {
      const row = db.prepare(`
        SELECT CASE WHEN COUNT(*) > @limit THEN COUNT(*) - @limit ELSE 0 END AS excess
        FROM steam_tasks
      `).get({ limit: STEAM_TASKS_MAX_ROWS });
      const excess = row && Number.isFinite(row.excess) ? row.excess : 0;
      if (!excess) return 0;

      const info = db.prepare(`
        DELETE FROM steam_tasks
        WHERE id IN (
          SELECT id FROM steam_tasks
          WHERE status NOT IN ('PENDING','RUNNING')
          ORDER BY created_at ASC, id ASC
          LIMIT @toDelete
        )
      `).run({ toDelete: excess });

      if (info && info.changes > 0) {
        log('info', 'Pruned steam_tasks rows', { deleted: info.changes, max_rows: STEAM_TASKS_MAX_ROWS, reason });
      }
      return info && info.changes ? info.changes : 0;
    } catch (err) {
      log('warn', 'Failed to prune steam_tasks', { error: err && err.message ? err.message : err, reason });
      return 0;
    }
  }

  installSteamTaskCapTrigger();
  pruneSteamTasks('startup');

  // ---------- Prepared Statements ----------
  const SENSITIVE_STEAM_TASK_TYPES = Object.freeze(['AUTH_GUARD_CODE', 'AUTH_LOGIN']);
  const sensitiveTaskTypeListSql = SENSITIVE_STEAM_TASK_TYPES
    .map((taskType) => `'${String(taskType).replace(/'/g, "''")}'`)
    .join(', ');
  const SENSITIVE_PENDING_TASK_TIMEOUT_S = 120;
  const selectPendingTaskStmt = db.prepare(`
    SELECT id, type, payload FROM steam_tasks
    WHERE status = 'PENDING'
    ORDER BY id ASC
    LIMIT 1
  `);
  const markTaskRunningRowStmt = db.prepare(`
    UPDATE steam_tasks
       SET status = 'RUNNING',
           started_at = ?,
           updated_at = ?,
           payload = CASE
             WHEN type IN (${sensitiveTaskTypeListSql}) THEN NULL
             ELSE payload
           END
     WHERE id = ? AND status = 'PENDING'
  `);
  const claimPendingTaskTxn = db.transaction((startedAt, updatedAt) => {
    const claimStartedAt = Number.isFinite(Number(startedAt))
      ? Number(startedAt)
      : Math.floor(Date.now() / 1000);
    const claimUpdatedAt = Number.isFinite(Number(updatedAt))
      ? Number(updatedAt)
      : claimStartedAt;

    for (let attempt = 0; attempt < 4; attempt += 1) {
      const task = selectPendingTaskStmt.get();
      if (!task) return null;

      const updated = markTaskRunningRowStmt.run(claimStartedAt, claimUpdatedAt, task.id);
      if (updated.changes) return task;
    }

    return null;
  });
  const markTaskRunningStmt = {
    get(startedAt, updatedAt) {
      return claimPendingTaskTxn(startedAt, updatedAt);
    },
    run(startedAt, updatedAt, taskId) {
      return markTaskRunningRowStmt.run(startedAt, updatedAt, taskId);
    },
  };
  const resetTaskPendingStmt = db.prepare(`
    UPDATE steam_tasks
       SET status = 'PENDING',
           started_at = NULL,
           updated_at = ?
     WHERE id = ?
  `);
  const STALE_TASK_TIMEOUT_S = 600;
  const failStaleTasksStmt = db.prepare(`
    UPDATE steam_tasks
       SET status = 'FAILED',
           payload = NULL,
           error = CASE
             WHEN status = 'RUNNING'
               THEN 'Task stale: keine Antwort innerhalb ' || @running_timeout_s || 's'
             ELSE 'Task stale: sensitiver Payload nach ' || @pending_timeout_s || 's ohne Abholung verworfen'
           END,
           finished_at = @now,
           updated_at = @now
     WHERE (
           status = 'RUNNING'
       AND started_at IS NOT NULL
       AND started_at < @running_cutoff
     ) OR (
           type IN (${sensitiveTaskTypeListSql})
       AND status = 'PENDING'
       AND payload IS NOT NULL
       AND updated_at < @pending_cutoff
     )
  `);
  const finishTaskStmt = db.prepare(`
    UPDATE steam_tasks
       SET status = ?,
           payload = NULL,
           result = ?,
           error = ?,
           finished_at = ?,
           updated_at = ?
     WHERE id = ?
  `);

  const selectHeroBuildSourceStmt = db.prepare(`
    SELECT * FROM hero_build_sources WHERE hero_build_id = ?
  `);
  const selectHeroBuildCloneMetaStmt = db.prepare(`
    SELECT target_name, target_description, target_language
      FROM hero_build_clones
     WHERE origin_hero_build_id = ?
  `);

  const selectPendingCommandStmt = db.prepare(`
    SELECT id, command, payload
      FROM standalone_commands
     WHERE bot = ?
       AND status = 'pending'
  ORDER BY id ASC
     LIMIT 1
  `);
  const markCommandRunningStmt = db.prepare(`
    UPDATE standalone_commands
       SET status = 'running',
           started_at = CURRENT_TIMESTAMP
     WHERE id = ?
       AND status = 'pending'
  `);
  const finalizeCommandStmt = db.prepare(`
    UPDATE standalone_commands
       SET status = ?,
           result = ?,
           error = ?,
           finished_at = CURRENT_TIMESTAMP
     WHERE id = ?
  `);

  const upsertStandaloneStateStmt = db.prepare(`
    INSERT INTO standalone_bot_state(bot, heartbeat, payload, updated_at)
    VALUES (@bot, @heartbeat, @payload, CURRENT_TIMESTAMP)
    ON CONFLICT(bot) DO UPDATE SET
      heartbeat = excluded.heartbeat,
      payload = excluded.payload,
      updated_at = CURRENT_TIMESTAMP
  `);
  const updateHeroBuildCloneUploadedStmt = db.prepare(`
    UPDATE hero_build_clones
       SET status = ?,
           status_info = ?,
           uploaded_build_id = ?,
           uploaded_version = ?,
           updated_at = strftime('%s','now')
     WHERE origin_hero_build_id = ?
  `);

  const steamTaskCountsStmt = db.prepare(`
    SELECT status, COUNT(*) AS count
      FROM steam_tasks
  GROUP BY status
  `);
  const steamTaskRecentStmt = db.prepare(`
    SELECT id, type, status, updated_at, finished_at
      FROM steam_tasks
  ORDER BY updated_at DESC
     LIMIT 10
  `);

  const steamLinksForSyncStmt = db.prepare(`
    SELECT steam_id, user_id, name, verified, is_steam_friend
      FROM steam_links
     WHERE steam_id IS NOT NULL
       AND steam_id != ''
  `);
  const selectFriendCheckCacheStmt = db.prepare(`
    SELECT friend, checked_at
      FROM steam_friend_check_cache
     WHERE steam_id=?
  `);
  const upsertFriendCheckCacheStmt = db.prepare(`
    INSERT INTO steam_friend_check_cache(steam_id, friend, checked_at)
    VALUES(?, ?, ?)
    ON CONFLICT(steam_id) DO UPDATE SET
      friend=excluded.friend,
      checked_at=excluded.checked_at
  `);
  const countFriendRequestsSentSinceStmt = db.prepare(`
    SELECT COUNT(*) AS c
      FROM steam_friend_requests
     WHERE status='sent'
       AND error IS NULL
       AND last_attempt IS NOT NULL
       AND last_attempt >= ?
  `);
  const upsertPendingFriendRequestStmt = db.prepare(`
    INSERT INTO steam_friend_requests(steam_id, status)
    VALUES(?, 'pending')
    ON CONFLICT(steam_id) DO UPDATE SET
      status='pending',
      last_attempt=CASE WHEN steam_friend_requests.status='sent' THEN NULL ELSE steam_friend_requests.last_attempt END,
      attempts=CASE WHEN steam_friend_requests.status='sent' THEN 0 ELSE COALESCE(steam_friend_requests.attempts, 0) END,
      error=NULL,
      requested_at=CASE WHEN steam_friend_requests.status='sent' THEN strftime('%s','now') ELSE steam_friend_requests.requested_at END
  `);
  const selectPendingFriendRequestForSteamIdStmt = db.prepare(`
    SELECT steam_id, requested_at, attempts, last_attempt
      FROM steam_friend_requests
     WHERE steam_id=?
       AND status IN ('pending', 'manual')
     LIMIT 1
  `);
  const selectFriendRequestBatchStmt = db.prepare(`
    SELECT steam_id, status, attempts, last_attempt
      FROM steam_friend_requests
     WHERE status = 'pending'
  ORDER BY COALESCE(last_attempt, 0) ASC
     LIMIT ?
  `);
  const markFriendRequestSentStmt = db.prepare(`
    UPDATE steam_friend_requests
       SET status='sent',
           last_attempt=?,
           attempts=COALESCE(attempts,0)+1,
           error=NULL
     WHERE steam_id=?
  `);
  const markFriendRequestSkippedStmt = db.prepare(`
    UPDATE steam_friend_requests
       SET status='sent',
           last_attempt=?,
           attempts=COALESCE(attempts,0),
           error='already_friend'
     WHERE steam_id=?
  `);
  const markFriendRequestFailedStmt = db.prepare(`
    UPDATE steam_friend_requests
       SET status='pending',
           last_attempt=?,
           attempts=COALESCE(attempts,0)+1,
           error=?
     WHERE steam_id=?
  `);
  const deleteFriendRequestStmt = db.prepare(`
    DELETE FROM steam_friend_requests WHERE steam_id=?
  `);
  const clearFriendFlagStmt = db.prepare(`
    UPDATE steam_links
       SET is_steam_friend = 0,
           verified = CASE WHEN verified != 0 THEN 0 ELSE verified END,
           updated_at = CURRENT_TIMESTAMP
     WHERE steam_id = ?
  `);
  const selectSteamLinkOwnersForSteamIdStmt = db.prepare(`
    SELECT DISTINCT CAST(user_id AS TEXT) AS user_id
      FROM steam_links
     WHERE steam_id = ?
       AND user_id != 0
  ORDER BY user_id ASC
  `);
  const verifySteamLinkForUserStmt = db.prepare(`
    UPDATE steam_links
    SET verified = 1,
        is_steam_friend = 1,
        name = COALESCE(NULLIF(@name, ''), name),
        updated_at = CURRENT_TIMESTAMP
    WHERE steam_id = @steam_id
      AND user_id = @user_id
  `);
  const unverifySteamLinkForUserStmt = db.prepare(`
    UPDATE steam_links
    SET verified = 0,
        is_steam_friend = 0,
        updated_at = CURRENT_TIMESTAMP
    WHERE steam_id = @steam_id
      AND user_id = @user_id
  `);
  const selectActiveVerifiedFriendLinkForUserStmt = db.prepare(`
    SELECT 1
      FROM steam_links
     WHERE user_id = ?
       AND verified = 1
       AND is_steam_friend = 1
     LIMIT 1
  `);
  const upsertRoleCleanupPendingStmt = db.prepare(`
    INSERT INTO steam_role_cleanup_pending(
      user_id, reason, attempts, last_error, created_at, updated_at
    )
    VALUES (?, ?, 0, NULL, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
      reason = excluded.reason,
      updated_at = excluded.updated_at
  `);

  return {
    db,
    resolveDbPath,
    ensureDir,
    installSteamTaskCapTrigger,
    pruneSteamTasks,
    STALE_TASK_TIMEOUT_S,
    SENSITIVE_PENDING_TASK_TIMEOUT_S,
    // Task statements
    selectPendingTaskStmt,
    markTaskRunningStmt,
    resetTaskPendingStmt,
    failStaleTasksStmt,
    finishTaskStmt,
    // Build statements
    selectHeroBuildSourceStmt,
    selectHeroBuildCloneMetaStmt,
    updateHeroBuildCloneUploadedStmt,
    // Command statements
    selectPendingCommandStmt,
    markCommandRunningStmt,
    finalizeCommandStmt,
    // State statements
    upsertStandaloneStateStmt,
    steamTaskCountsStmt,
    steamTaskRecentStmt,
    // Friend/link statements
    steamLinksForSyncStmt,
    selectFriendCheckCacheStmt,
    upsertFriendCheckCacheStmt,
    countFriendRequestsSentSinceStmt,
    upsertPendingFriendRequestStmt,
    selectPendingFriendRequestForSteamIdStmt,
    selectFriendRequestBatchStmt,
    markFriendRequestSentStmt,
    markFriendRequestSkippedStmt,
    markFriendRequestFailedStmt,
    deleteFriendRequestStmt,
    clearFriendFlagStmt,
    selectSteamLinkOwnersForSteamIdStmt,
    verifySteamLinkForUserStmt,
    unverifySteamLinkForUserStmt,
    selectActiveVerifiedFriendLinkForUserStmt,
    upsertRoleCleanupPendingStmt,
  };
};
