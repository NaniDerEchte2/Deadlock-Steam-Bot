'use strict';

/**
 * GC scheduler for profile-card operations.
 * Provides:
 * - bounded queue
 * - bounded parallelism
 * - minimum dispatch interval
 * - timeout retries with exponential backoff + jitter
 * - circuit breaker with half-open recovery
 * - telemetry snapshot
 */

function toFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBoundedInt(value, fallback, minimum, maximum = null) {
  const base = Math.floor(toFiniteNumber(value, fallback));
  const lower = Number.isFinite(minimum) ? minimum : 0;
  const upper = Number.isFinite(maximum) ? maximum : null;
  if (upper !== null) return Math.min(upper, Math.max(lower, base));
  return Math.max(lower, base);
}

function ratePercent(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) return 0;
  return Math.max(0, Math.min(100, (numerator / denominator) * 100));
}

function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function nowMs() {
  return Date.now();
}

function buildError(message, code, details = {}) {
  const err = new Error(message);
  err.code = code;
  Object.assign(err, details);
  return err;
}

function resolveTimeoutErrorChecker(checker) {
  if (typeof checker === 'function') return checker;
  return (err) => {
    if (!err) return false;
    return String(err && err.message ? err.message : err).toLowerCase().includes('timeout');
  };
}

function createGcScheduler(ctx = {}) {
  const {
    gcProfileCard,
    requestDeadlockGcTokens,
    waitForDeadlockGcReady,
    sleep,
    isTimeoutError,
    log,
    trace,
  } = ctx;

  const logger = typeof log === 'function' ? log : () => {};
  const tracer = typeof trace === 'function' ? trace : () => {};
  const wait = typeof sleep === 'function' ? sleep : defaultSleep;
  const checkTimeoutError = resolveTimeoutErrorChecker(isTimeoutError);

  const config = {
    maxQueueLength: toBoundedInt(process.env.DEADLOCK_GC_QUEUE_MAX_LENGTH, 250, 1, 5000),
    parallelism: toBoundedInt(process.env.DEADLOCK_GC_QUEUE_PARALLELISM, 1, 1, 12),
    minIntervalMs: toBoundedInt(process.env.DEADLOCK_GC_MIN_INTERVAL_MS, 250, 0, 15000),
    timeoutRetries: toBoundedInt(process.env.DEADLOCK_GC_TIMEOUT_RETRIES, 2, 0, 8),
    retryBackoffBaseMs: toBoundedInt(process.env.DEADLOCK_GC_TIMEOUT_BACKOFF_BASE_MS, 500, 50, 30000),
    retryBackoffMaxMs: toBoundedInt(process.env.DEADLOCK_GC_TIMEOUT_BACKOFF_MAX_MS, 10000, 250, 120000),
    retryJitterMs: toBoundedInt(process.env.DEADLOCK_GC_TIMEOUT_JITTER_MS, 250, 0, 20000),
    breakerWindowMs: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_WINDOW_MS, 60000, 5000, 600000),
    breakerOpenMs: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_OPEN_MS, 30000, 1000, 300000),
    breakerTimeoutThreshold: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_TIMEOUT_THRESHOLD, 5, 2, 200),
    breakerMinSamples: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_MIN_SAMPLES, 6, 2, 500),
    breakerTimeoutRateThreshold: Math.max(
      0.1,
      Math.min(1, toFiniteNumber(process.env.DEADLOCK_GC_BREAKER_TIMEOUT_RATE, 0.6))
    ),
    halfOpenMaxInFlight: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_HALF_OPEN_MAX_IN_FLIGHT, 1, 1, 4),
    halfOpenSuccessThreshold: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_HALF_OPEN_SUCCESS_THRESHOLD, 1, 1, 12),
    breakerMaxRecentOutcomes: toBoundedInt(process.env.DEADLOCK_GC_BREAKER_MAX_RECENT_OUTCOMES, 4000, 500, 200000),
    defaultProfileCardTimeoutMs: toBoundedInt(process.env.DEADLOCK_GC_PROFILE_CARD_TIMEOUT_MS, 15000, 3000, 120000),
  };

  const queue = [];
  let queueId = 0;
  let nextDispatchAt = 0;
  let pumpTimer = null;

  const breaker = {
    status: 'closed',
    openedAt: 0,
    openUntil: 0,
    halfOpenInFlight: 0,
    halfOpenSuccesses: 0,
    openCount: 0,
    lastReason: null,
  };

  const stats = {
    startedAt: nowMs(),
    processed: 0,
    success: 0,
    failed: 0,
    timeoutFailures: 0,
    retries: 0,
    rejectedByBreaker: 0,
    rejectedByQueue: 0,
    inFlight: 0,
    recentOutcomes: [],
  };

  function logEvent(level, message, extra) {
    try { logger(level, message, extra); } catch (_) {}
  }

  function traceEvent(event, details) {
    try { tracer(event, details); } catch (_) {}
  }

  function isBreakerOpen(ts = nowMs()) {
    return breaker.status === 'open' && ts < breaker.openUntil;
  }

  function setBreakerStatus(nextStatus, reason, details = {}) {
    const previous = breaker.status;
    breaker.status = nextStatus;
    breaker.lastReason = reason || null;
    if (nextStatus === 'open') {
      breaker.openedAt = nowMs();
      breaker.openUntil = breaker.openedAt + config.breakerOpenMs;
      breaker.openCount += 1;
      breaker.halfOpenInFlight = 0;
      breaker.halfOpenSuccesses = 0;
    } else if (nextStatus === 'closed') {
      breaker.openUntil = 0;
      breaker.halfOpenInFlight = 0;
      breaker.halfOpenSuccesses = 0;
    } else if (nextStatus === 'half_open') {
      breaker.halfOpenInFlight = 0;
      breaker.halfOpenSuccesses = 0;
    }

    logEvent('warn', 'GC scheduler breaker transition', {
      previous,
      status: breaker.status,
      reason: reason || 'none',
      ...details,
    });
    traceEvent('gc_scheduler_breaker_transition', {
      previous,
      status: breaker.status,
      reason: reason || 'none',
      ...details,
    });
  }

  function pruneRecentOutcomes(ts = nowMs()) {
    const cutoff = ts - config.breakerWindowMs;
    while (stats.recentOutcomes.length && stats.recentOutcomes[0].ts < cutoff) {
      stats.recentOutcomes.shift();
    }
  }

  function addOutcome(kind, ts = nowMs()) {
    stats.recentOutcomes.push({ ts, kind });
    pruneRecentOutcomes(ts);
    const overflow = stats.recentOutcomes.length - config.breakerMaxRecentOutcomes;
    if (overflow > 0) stats.recentOutcomes.splice(0, overflow);
  }

  function collectRecentCounters(ts = nowMs()) {
    pruneRecentOutcomes(ts);
    const counters = {
      total: 0,
      success: 0,
      timeout: 0,
      failed: 0,
    };
    for (const row of stats.recentOutcomes) {
      counters.total += 1;
      if (row.kind === 'success') counters.success += 1;
      if (row.kind === 'timeout') {
        counters.timeout += 1;
        counters.failed += 1;
      } else if (row.kind === 'failed') {
        counters.failed += 1;
      }
    }
    return counters;
  }

  function maybeOpenBreakerForTimeoutStorm() {
    if (breaker.status !== 'closed') return;
    const recent = collectRecentCounters();
    if (recent.total < config.breakerMinSamples) return;
    if (recent.timeout < config.breakerTimeoutThreshold) return;
    const timeoutRate = recent.total > 0 ? recent.timeout / recent.total : 0;
    if (timeoutRate < config.breakerTimeoutRateThreshold) return;
    setBreakerStatus('open', 'timeout_storm', {
      timeoutRate,
      recentTotal: recent.total,
      recentTimeout: recent.timeout,
    });
  }

  function randomJitter(maxJitterMs) {
    if (!Number.isFinite(maxJitterMs) || maxJitterMs <= 0) return 0;
    return Math.floor(Math.random() * (Math.floor(maxJitterMs) + 1));
  }

  async function ensureGcReadyForJob(job) {
    if (job.requireGcReady === false) return true;
    if (typeof waitForDeadlockGcReady !== 'function') {
      throw new Error('waitForDeadlockGcReady unavailable');
    }
    return waitForDeadlockGcReady(job.gcReadyTimeoutMs, {
      retryAttempts: job.gcRetryAttempts,
    });
  }

  async function executeProfileCardJob(job) {
    if (!gcProfileCard || typeof gcProfileCard.fetchPlayerCard !== 'function') {
      throw new Error('GC profile-card service unavailable');
    }
    if (typeof requestDeadlockGcTokens === 'function') {
      requestDeadlockGcTokens(`gc_scheduler:${job.type}`).catch(() => {});
    }
    await ensureGcReadyForJob(job);
    return gcProfileCard.fetchPlayerCard(job.fetchOptions);
  }

  async function runJobWithRetry(job) {
    const retries = toBoundedInt(job.timeoutRetries, config.timeoutRetries, 0, 10);
    let attempt = 0;
    while (attempt <= retries) {
      try {
        return await executeProfileCardJob(job);
      } catch (err) {
        const timeoutError = checkTimeoutError(err);
        if (!timeoutError || attempt >= retries) throw err;
        const backoff = Math.min(
          config.retryBackoffMaxMs,
          config.retryBackoffBaseMs * (2 ** attempt)
        );
        const waitMs = backoff + randomJitter(config.retryJitterMs);
        stats.retries += 1;
        logEvent('warn', 'GC scheduler timeout retry', {
          queue_job_id: job.id,
          type: job.type,
          attempt: attempt + 1,
          retries,
          waitMs,
          error: err && err.message ? err.message : String(err),
        });
        traceEvent('gc_scheduler_timeout_retry', {
          queue_job_id: job.id,
          type: job.type,
          attempt: attempt + 1,
          retries,
          waitMs,
        });
        await wait(waitMs);
      }
      attempt += 1;
    }
    throw new Error('GC scheduler retry loop exhausted');
  }

  function schedulePump(delayMs = 0) {
    const delay = Math.max(0, Number(delayMs) || 0);
    if (pumpTimer) {
      clearTimeout(pumpTimer);
      pumpTimer = null;
    }
    pumpTimer = setTimeout(() => {
      pumpTimer = null;
      dispatchNext();
    }, delay);
  }

  function dequeueJob() {
    if (!queue.length) return null;
    return queue.shift();
  }

  function completeJob(job, result, error) {
    if (error) job.reject(error);
    else job.resolve(result);
  }

  async function runQueuedJob(job) {
    stats.processed += 1;
    stats.inFlight += 1;
    if (breaker.status === 'half_open') {
      breaker.halfOpenInFlight += 1;
    }

    try {
      const result = await runJobWithRetry(job);
      stats.success += 1;
      addOutcome('success');
      if (breaker.status === 'half_open') {
        breaker.halfOpenSuccesses += 1;
      }
      completeJob(job, result, null);
    } catch (err) {
      const timeoutError = checkTimeoutError(err);
      stats.failed += 1;
      if (timeoutError) {
        stats.timeoutFailures += 1;
        addOutcome('timeout');
        if (breaker.status === 'half_open') {
          setBreakerStatus('open', 'half_open_probe_timeout', {
            queueLength: queue.length,
            error: err && err.message ? err.message : String(err),
          });
        } else {
          maybeOpenBreakerForTimeoutStorm();
        }
      } else {
        addOutcome('failed');
        if (breaker.status === 'half_open') {
          setBreakerStatus('open', 'half_open_probe_failed', {
            queueLength: queue.length,
            error: err && err.message ? err.message : String(err),
          });
        }
      }
      completeJob(job, null, err);
    } finally {
      stats.inFlight = Math.max(0, stats.inFlight - 1);
      if (breaker.status === 'half_open') {
        breaker.halfOpenInFlight = Math.max(0, breaker.halfOpenInFlight - 1);
        const required = Math.max(1, config.halfOpenSuccessThreshold);
        if (breaker.halfOpenInFlight === 0 && breaker.halfOpenSuccesses >= required) {
          setBreakerStatus('closed', 'half_open_probe_threshold_met', {
            queueLength: queue.length,
            successfulProbes: breaker.halfOpenSuccesses,
            requiredSuccessfulProbes: required,
          });
        }
      }
      dispatchNext();
    }
  }

  function dispatchNext() {
    if (stats.inFlight >= config.parallelism) return;

    const ts = nowMs();
    if (breaker.status === 'open') {
      if (ts < breaker.openUntil) {
        schedulePump(Math.max(10, breaker.openUntil - ts));
        return;
      }
      setBreakerStatus('half_open', 'open_cooldown_elapsed', { queueLength: queue.length });
    }

    if (!queue.length) return;

    if (breaker.status === 'half_open' && breaker.halfOpenInFlight >= config.halfOpenMaxInFlight) {
      schedulePump(50);
      return;
    }

    if (ts < nextDispatchAt) {
      schedulePump(Math.max(10, nextDispatchAt - ts));
      return;
    }

    const job = dequeueJob();
    if (!job) return;

    nextDispatchAt = ts + config.minIntervalMs;
    void runQueuedJob(job);

    if (queue.length && stats.inFlight < config.parallelism) {
      schedulePump(config.minIntervalMs > 0 ? config.minIntervalMs : 0);
    }
  }

  function enqueue(jobSpec = {}) {
    return new Promise((resolve, reject) => {
      const ts = nowMs();
      if (isBreakerOpen(ts)) {
        stats.rejectedByBreaker += 1;
        return reject(buildError(
          'GC scheduler circuit breaker is open',
          'GC_BREAKER_OPEN',
          { openUntil: breaker.openUntil }
        ));
      }
      if (queue.length >= config.maxQueueLength) {
        stats.rejectedByQueue += 1;
        return reject(buildError(
          'GC scheduler queue is full',
          'GC_QUEUE_FULL',
          { queueLength: queue.length, maxQueueLength: config.maxQueueLength }
        ));
      }

      queueId += 1;
      const job = {
        id: queueId,
        type: String(jobSpec.type || 'gc_profile_card'),
        enqueuedAt: ts,
        requireGcReady: jobSpec.requireGcReady !== false,
        gcReadyTimeoutMs: jobSpec.gcReadyTimeoutMs,
        gcRetryAttempts: jobSpec.gcRetryAttempts,
        timeoutRetries: jobSpec.timeoutRetries,
        fetchOptions: jobSpec.fetchOptions || {},
        resolve,
        reject,
      };
      queue.push(job);

      traceEvent('gc_scheduler_enqueue', {
        queue_job_id: job.id,
        type: job.type,
        queueLength: queue.length,
      });

      dispatchNext();
    });
  }

  function fetchProfileCard(options = {}) {
    const accountId = toBoundedInt(options.accountId, NaN, 1, null);
    if (!Number.isFinite(accountId) || accountId <= 0) {
      return Promise.reject(new Error('account_id missing or invalid'));
    }
    const timeoutMs = toBoundedInt(
      options.timeoutMs,
      config.defaultProfileCardTimeoutMs,
      3000,
      120000
    );

    return enqueue({
      type: options.type || 'profile_card',
      requireGcReady: options.requireGcReady !== false,
      gcReadyTimeoutMs: options.gcReadyTimeoutMs,
      gcRetryAttempts: options.gcRetryAttempts,
      timeoutRetries: options.timeoutRetries,
      fetchOptions: {
        accountId,
        timeoutMs,
        friendAccessHint: options.friendAccessHint !== false,
        devAccessHint: options.devAccessHint,
      },
    });
  }

  function getTelemetrySnapshot() {
    const recent = collectRecentCounters();
    const completed = stats.success + stats.failed;
    return {
      queueLength: queue.length,
      inFlight: stats.inFlight,
      timeoutRate: Number(ratePercent(recent.timeout, recent.total).toFixed(2)),
      successRate: Number(ratePercent(stats.success, completed).toFixed(2)),
      breakerStatus: breaker.status,
      breakerOpenUntil: breaker.openUntil || null,
      breakerLastReason: breaker.lastReason || null,
      totals: {
        processed: stats.processed,
        success: stats.success,
        failed: stats.failed,
        timeoutFailures: stats.timeoutFailures,
        retries: stats.retries,
        rejectedByBreaker: stats.rejectedByBreaker,
        rejectedByQueue: stats.rejectedByQueue,
      },
      config: {
        parallelism: config.parallelism,
        minIntervalMs: config.minIntervalMs,
        maxQueueLength: config.maxQueueLength,
      },
    };
  }

  return {
    enqueue,
    fetchProfileCard,
    getTelemetrySnapshot,
  };
}

module.exports = {
  createGcScheduler,
};
