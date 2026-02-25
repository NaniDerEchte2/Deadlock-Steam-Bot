'use strict';

module.exports = (context) => {
  Object.assign(globalThis, context);

// ---------- Task Dispatcher (Promise-fähig) ----------
let taskInProgress = false;

function finalizeTaskRun(task, outcome) {
  // outcome kann sync (Objekt) oder Promise sein
  if (outcome && typeof outcome.then === 'function') {
    outcome.then(
      (res) => completeTask(task.id, (res && res.ok) ? 'DONE' : 'FAILED', res, res && !res.ok ? res.error : null),
      (err) => completeTask(task.id, 'FAILED', { ok: false, error: err?.message || String(err) }, err?.message || String(err))
    ).finally(() => { taskInProgress = false; });
    return true; // async
  } else {
    const ok = outcome && outcome.ok;
    completeTask(task.id, ok ? 'DONE' : 'FAILED', outcome, outcome && !ok ? outcome.error : null);
    return false; // sync
  }
}

function processNextTask() {
  if (taskInProgress) return;
  taskInProgress = true;

  let task = null;
  let isAsync = false;
  try {
    task = selectPendingTaskStmt.get();
    if (!task) return;

    const startedAt = nowSeconds();
    const updated = markTaskRunningStmt.run(startedAt, startedAt, task.id);
    if (!updated.changes) return;

    const payload = safeJsonParse(task.payload);
    log('info', 'Executing steam task', { id: task.id, type: task.type });

    switch (task.type) {
      case 'AUTH_STATUS':
        finalizeTaskRun(task, { ok: true, data: getStatusPayload() });
        break;
      case 'AUTH_LOGIN':
        finalizeTaskRun(task, { ok: true, data: initiateLogin('task', payload) });
        break;
      case 'AUTH_GUARD_CODE':
        finalizeTaskRun(task, { ok: true, data: handleGuardCodeTask(payload) });
        break;
      case 'AUTH_LOGOUT':
        finalizeTaskRun(task, { ok: true, data: handleLogoutTask() });
        break;

      case 'AUTH_REFRESH_GAME_VERSION': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');
          const success = await deadlockGcBot.refreshGameVersion();
          return {
            ok: success,
            data: {
              version: deadlockGcBot.sessionNeed,
              updated: success
            }
          };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'AUTH_SEND_FRIEND_REQUEST': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');
          const raw = payload?.steam_id ?? payload?.steam_id64;
          const sid = parseSteamID(raw);
          const sid64 = typeof sid.getSteamID64 === 'function' ? sid.getSteamID64() : String(sid);
          if (await isAlreadyFriend(sid64)) {
            log('info', 'AUTH_SEND_FRIEND_REQUEST skipped - already friends', { steam_id64: sid64 });
            return {
              ok: true,
              data: {
                steam_id64: sid64,
                account_id: sid.accountid ?? null,
                skipped: true,
                reason: 'already_friend',
              },
            };
          }

          await sendFriendRequest(sid);
          return {
            ok: true,
            data: {
              steam_id64: sid64,
              account_id: sid.accountid ?? null,
            },
          };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'AUTH_CHECK_FRIENDSHIP': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');
          const raw = payload?.steam_id ?? payload?.steam_id64;
          const sid = parseSteamID(raw);
          const sid64 = typeof sid.getSteamID64 === 'function' ? sid.getSteamID64() : String(sid);
          let relationshipRaw = client.myFriends ? client.myFriends[sid64] : undefined;
          let friendSource = 'client';
          let isFriend = Number(relationshipRaw) === Number((SteamUser.EFriendRelationship || {}).Friend);

          if (!isFriend) {
            const viaWeb = await isFriendViaWebApi(sid64);
            if (viaWeb && viaWeb.friend) {
              isFriend = true;
              friendSource = viaWeb.source || 'webapi';
              if (relationshipRaw === undefined) {
                if (SteamUser.EFriendRelationship && Object.prototype.hasOwnProperty.call(SteamUser.EFriendRelationship, 'Friend')) {
                  relationshipRaw = SteamUser.EFriendRelationship.Friend;
                } else {
                  relationshipRaw = 'Friend';
                }
              }
            }
          }

          return {
            ok: true,
            data: {
              steam_id64: sid64,
              account_id: sid.accountid ?? null,
              friend: isFriend,
              relationship: relationshipRaw ?? null,
              relationship_name: relationshipName(relationshipRaw),
              friend_source: friendSource,
              webapi_cache_age_ms: getWebApiFriendCacheAgeMs(),
            },
          };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'AUTH_REMOVE_FRIEND': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');
          const raw = payload?.steam_id ?? payload?.steam_id64;
          const result = await removeFriendship(raw);
          return { ok: true, data: result };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'BUILD_PUBLISH': {
        // Check if another build publish is already in progress
        if (state.heroBuildPublishWaiter) {
          log('info', 'Build publish already in progress, requeueing task', { id: task.id });
          resetTaskPendingStmt.run(nowSeconds(), task.id);
          break;
        }

        const promise = (async () => {
          try {
            log('info', 'BUILD_PUBLISH: Starting', { task_id: task.id, origin_id: payload?.origin_hero_build_id });

            if (!runtimeState.logged_on) throw new Error('Not logged in');

            log('info', 'BUILD_PUBLISH: Loading proto');
            await loadHeroBuildProto();

            const originId = payload?.origin_hero_build_id ?? payload?.hero_build_id;
            if (!originId) throw new Error('origin_hero_build_id missing');

            log('info', 'BUILD_PUBLISH: Fetching build source', { originId });
            const src = selectHeroBuildSourceStmt.get(originId);
            if (!src) throw new Error(`hero_build_sources missing for ${originId}`);

            log('info', 'BUILD_PUBLISH: Fetching clone meta', { originId });
            const cloneMeta = selectHeroBuildCloneMetaStmt.get(originId) || {};

            log('info', 'BUILD_PUBLISH: Building metadata', {
              cloneMeta: cloneMeta ? Object.keys(cloneMeta) : 'none'
            });
            const targetName = payload?.target_name || cloneMeta.target_name;
            const targetDescription = payload?.target_description || cloneMeta.target_description;
            const targetLanguage = safeNumber(payload?.target_language) ?? safeNumber(cloneMeta.target_language) ?? 1;
            const authorAccountId = client?.steamID?.accountid ? Number(client.steamID.accountid) : undefined;
            const useMinimal = payload?.minimal === true;
            const useUpdate = payload?.update === true;
            const minimalUpdate = payload?.minimal_update === true;
            const meta = {
                target_name: targetName,
                target_description: targetDescription,
                target_language: targetLanguage,
                author_account_id: useUpdate ? safeNumber(src.author_account_id) : authorAccountId,
                origin_build_id: src.hero_build_id,
            };
            let heroBuild;
            if (useUpdate) {
                heroBuild = await buildUpdateHeroBuild(src, meta);
                if (minimalUpdate) {
                heroBuild.tags = [];
                heroBuild.details = { mod_categories: [] };
                }
            } else if (useMinimal) {
                heroBuild = await buildMinimalHeroBuild(src, meta);
            } else {
                heroBuild = await mapHeroBuildFromRow(src, meta);
            }
            if (!useUpdate) {
              // new build => clear hero_build_id so GC assigns fresh
              delete heroBuild.hero_build_id;
            }
            log('info', 'BUILD_PUBLISH: Building hero object', {
                useMinimal,
                useUpdate,
                minimalUpdate,
            });

            log('info', 'Publishing hero build', {
                originId,
                heroId: heroBuild.hero_id,
                author: heroBuild.author_account_id,
                language: heroBuild.language,
                name: heroBuild.name,
                mode: useUpdate ? (minimalUpdate ? 'update-minimal' : 'update') : (useMinimal ? 'new-minimal' : 'new'),
                hero_build_id: heroBuild.hero_build_id,
            });

            log('info', 'BUILD_PUBLISH: Calling sendHeroBuildUpdate');
            log('info', 'BUILD_PUBLISH: heroBuild object', { heroBuild: JSON.stringify(heroBuild) });
            const resp = await sendHeroBuildUpdate(heroBuild);

            log('info', 'BUILD_PUBLISH: Update successful', { resp });
            updateHeroBuildCloneUploadedStmt.run('done', null, resp.heroBuildId || null, resp.version || null, originId);
            return { ok: true, response: resp, origin_id: originId };
          } catch (err) {
            log('error', 'BUILD_PUBLISH: Failed', {
              task_id: task.id,
              origin_id: payload?.origin_hero_build_id,
              error: err?.message || String(err),
              stack: err?.stack || 'no stack'
            });
            throw err;
          }
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'AUTH_SEND_PLAYTEST_INVITE': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');
          const raw = payload?.steam_id ?? payload?.steam_id64;
          const timeoutMs = payload?.timeout_ms ?? payload?.response_timeout_ms;
          const inviteRetryAttempts = payload?.retry_attempts ?? payload?.invite_retry_attempts ?? payload?.attempts;
          const gcReadyRetryAttempts = payload?.gc_ready_retry_attempts ?? payload?.gc_retry_attempts;
          const gcReadyTimeoutMs = payload?.gc_ready_timeout_ms ?? payload?.gc_timeout_ms;
          const sid = raw ? parseSteamID(raw) : null;
          const accountId = payload?.account_id != null ? Number(payload.account_id) : (sid ? sid.accountid : null);
          if (!Number.isFinite(accountId) || accountId <= 0) throw new Error('account_id missing or invalid');
          const locationRaw = typeof payload?.location === 'string' ? payload.location.trim() : '';
          const location = locationRaw || 'discord-betainvite';
          const inviteTimeout = Number(timeoutMs);
          const response = await sendPlaytestInvite(
            Number(accountId),
            location,
            Number.isFinite(inviteTimeout) ? inviteTimeout : undefined,
            {
              retryAttempts: Number.isFinite(Number(inviteRetryAttempts)) ? Number(inviteRetryAttempts) : undefined,
              gcRetryAttempts: Number.isFinite(Number(gcReadyRetryAttempts)) ? Number(gcReadyRetryAttempts) : undefined,
              gcTimeoutMs: Number.isFinite(Number(gcReadyTimeoutMs)) ? Number(gcReadyTimeoutMs) : undefined,
            }
          );
          const sid64 = sid && typeof sid.getSteamID64 === 'function' ? sid.getSteamID64() : (sid ? String(sid) : null);
          const success = Boolean(response && response.success);
          const errorText = success
            ? null
            : formatPlaytestError(response) || 'Game Coordinator hat die Einladung abgelehnt.';
          const data = {
            steam_id64: sid64,
            account_id: Number(accountId),
            location,
            response,
          };
          return success
            ? { ok: true, data }
            : { ok: false, data, error: errorText };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'AUTH_GET_FRIENDS_LIST': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');

          // Prefer native Steam client friend cache (no API key required),
          // and merge WebAPI data when available.
          const { ids: friendIds, clientCount, webCount } = await collectKnownFriendIds();

          const friends = [];
          for (const steamId64 of friendIds) {
            friends.push({
              steam_id64: steamId64,
              // Try to get account_id from steamID
              account_id: null, // We'll compute this on Python side if needed
            });
          }

          return {
            ok: true,
            data: {
              count: friends.length,
              source: {
                client_count: Number(clientCount) || 0,
                webapi_count: Number(webCount) || 0,
              },
              friends: friends,
            },
          };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      case 'GC_GET_PROFILE_CARD': {
        const promise = (async () => {
          if (!runtimeState.logged_on) throw new Error('Not logged in');

          const accountIdRaw = payload?.account_id;
          const steamInput = payload?.steam_id ?? payload?.steam_id64;
          const requestTimeoutRaw = payload?.timeout_ms ?? payload?.request_timeout_ms;
          const gcReadyTimeoutRaw = payload?.gc_ready_timeout_ms ?? payload?.gc_timeout_ms;
          const gcRetryRaw = payload?.gc_ready_retry_attempts ?? payload?.gc_retry_attempts;

          let sid = null;
          if (steamInput !== undefined && steamInput !== null && String(steamInput).trim()) {
            sid = parseSteamID(steamInput);
          }

          const accountId = accountIdRaw != null
            ? Number(accountIdRaw)
            : (sid ? Number(sid.accountid) : null);
          if (!Number.isFinite(accountId) || accountId <= 0) {
            throw new Error('account_id missing or invalid');
          }

          const gcTimeoutMs = Number.isFinite(Number(gcReadyTimeoutRaw))
            ? Number(gcReadyTimeoutRaw)
            : DEFAULT_GC_READY_TIMEOUT_MS;
          const gcRetryAttempts = Number.isFinite(Number(gcRetryRaw))
            ? Number(gcRetryRaw)
            : DEFAULT_GC_READY_ATTEMPTS;

          await waitForDeadlockGcReady(gcTimeoutMs, { retryAttempts: gcRetryAttempts });

          const timeoutMs = Number.isFinite(Number(requestTimeoutRaw))
            ? Number(requestTimeoutRaw)
            : undefined;
          const profileCard = await gcProfileCard.fetchPlayerCard({
            accountId: Number(accountId),
            timeoutMs,
            friendAccessHint: payload?.friend_access_hint !== false,
            devAccessHint: payload?.dev_access_hint,
          });

          const steamId64 = sid && typeof sid.getSteamID64 === 'function'
            ? sid.getSteamID64()
            : null;

          return {
            ok: true,
            data: {
              steam_id64: steamId64,
              account_id: Number(accountId),
              card: profileCard,
            },
          };
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      // ========== BUILD DISCOVERY (via GC) ==========
      // Discover builds from watched authors using In-Game GC (replaces external API)
      case 'DISCOVER_WATCHED_BUILDS': {
        const promise = (async () => {
          log('info', 'DISCOVER_WATCHED_BUILDS: Starting build discovery via GC');

          if (!state.deadlockGcReady) {
            log('warn', 'DISCOVER_WATCHED_BUILDS: Deadlock GC not ready');
            return { ok: false, error: 'Deadlock GC not connected. Please wait for GC connection.' };
          }

          if (!buildCatalogManager) {
            log('error', 'DISCOVER_WATCHED_BUILDS: BuildCatalogManager not initialized');
            return { ok: false, error: 'BuildCatalogManager not initialized' };
          }

          try {
            const result = await buildCatalogManager.discoverWatchedBuilds();
            return {
              ok: result.success,
              data: {
                authors_checked: result.authorsChecked,
                builds_discovered: result.totalNewBuilds + result.totalUpdatedBuilds,
                new_builds: result.totalNewBuilds,
                updated_builds: result.totalUpdatedBuilds,
                errors: result.errors?.length || 0
              }
            };
          } catch (err) {
            log('error', 'DISCOVER_WATCHED_BUILDS: Failed', { error: err.message });
            return { ok: false, error: err.message };
          }
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      // ========== HERO-BASED BUILD DISCOVERY ==========
      // Alternative discovery: search by hero instead of author (works when author-search times out)
      case 'DISCOVER_BUILDS_VIA_HEROES': {
        const promise = (async () => {
          log('info', 'DISCOVER_BUILDS_VIA_HEROES: Starting HERO-based build discovery via GC');

          if (!state.deadlockGcReady) {
            log('warn', 'DISCOVER_BUILDS_VIA_HEROES: Deadlock GC not ready');
            return { ok: false, error: 'Deadlock GC not connected. Please wait for GC connection.' };
          }

          if (!buildCatalogManager) {
            log('error', 'DISCOVER_BUILDS_VIA_HEROES: BuildCatalogManager not initialized');
            return { ok: false, error: 'BuildCatalogManager not initialized' };
          }

          try {
            const result = await buildCatalogManager.discoverWatchedBuildsViaHeroes();
            return {
              ok: result.success,
              data: {
                heroes_checked: result.heroesChecked,
                heroes_with_builds: result.heroesWithBuilds,
                matched_builds: result.totalMatchedBuilds,
                new_builds: result.totalNewBuilds,
                updated_builds: result.totalUpdatedBuilds,
                errors: result.errors?.length || 0
              }
            };
          } catch (err) {
            log('error', 'DISCOVER_BUILDS_VIA_HEROES: Failed', { error: err.message });
            return { ok: false, error: err.message };
          }
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      // ========== CATALOG MAINTENANCE ==========
      // Maintains the catalog: selects builds, creates/updates German clones
      case 'MAINTAIN_BUILD_CATALOG': {
        const promise = (async () => {
          log('info', 'MAINTAIN_BUILD_CATALOG: Starting catalog maintenance');

          if (!buildCatalogManager) {
            log('error', 'MAINTAIN_BUILD_CATALOG: BuildCatalogManager not initialized');
            return { ok: false, error: 'BuildCatalogManager not initialized' };
          }

          try {
            const result = await buildCatalogManager.maintainCatalog();
            return {
              ok: result.success,
              data: {
                builds_to_clone: result.buildsToClone,
                builds_to_update: result.buildsToUpdate,
                tasks_created: result.tasksCreated,
                skipped_builds: result.skippedBuilds
              }
            };
          } catch (err) {
            log('error', 'MAINTAIN_BUILD_CATALOG: Failed', { error: err.message });
            return { ok: false, error: err.message };
          }
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      // ========== FULL CATALOG CYCLE ==========
      // Runs discovery + maintenance in one task
      case 'BUILD_CATALOG_CYCLE': {
        const promise = (async () => {
          log('info', 'BUILD_CATALOG_CYCLE: Starting full catalog cycle');

          if (!state.deadlockGcReady) {
            log('warn', 'BUILD_CATALOG_CYCLE: Deadlock GC not ready');
            return { ok: false, error: 'Deadlock GC not connected. Please wait for GC connection.' };
          }

          if (!buildCatalogManager) {
            log('error', 'BUILD_CATALOG_CYCLE: BuildCatalogManager not initialized');
            return { ok: false, error: 'BuildCatalogManager not initialized' };
          }

          try {
            const result = await buildCatalogManager.runFullCycle();
            return {
              ok: result.success,
              data: {
                discovery: result.discovery,
                maintenance: result.maintenance
              }
            };
          } catch (err) {
            log('error', 'BUILD_CATALOG_CYCLE: Failed', { error: err.message });
            return { ok: false, error: err.message };
          }
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      // ========== GC BUILD SEARCH (manual) ==========
      // Search for builds directly via the Deadlock Game Coordinator
      case 'GC_SEARCH_BUILDS': {
        const promise = (async () => {
          log('info', 'GC_SEARCH_BUILDS: Starting In-Game build search');

          if (!state.deadlockGcReady) {
            log('warn', 'GC_SEARCH_BUILDS: Deadlock GC not ready');
            return { ok: false, error: 'Deadlock GC not connected. Please wait for GC connection.' };
          }

          const searchOptions = {};
          if (payload.author_account_id) searchOptions.authorAccountId = payload.author_account_id;
          if (payload.hero_id) searchOptions.heroId = payload.hero_id;
          if (payload.search_text) searchOptions.searchText = payload.search_text;
          if (payload.hero_build_id) searchOptions.heroBuildId = payload.hero_build_id;
          if (payload.languages) searchOptions.languages = payload.languages;
          if (payload.tags) searchOptions.tags = payload.tags;

          log('info', 'GC_SEARCH_BUILDS: Searching with options', searchOptions);

          try {
            const response = await gcBuildSearch.findBuilds(searchOptions);

            const responseCode = response.response;
            const results = response.results || [];

            log('info', 'GC_SEARCH_BUILDS: Got response', {
              responseCode,
              resultCount: results.length
            });

            if (responseCode !== 1) { // k_eSuccess
              return {
                ok: false,
                error: `GC returned error code: ${responseCode}`,
                responseCode
              };
            }

            // Process and store the builds
            let newBuilds = 0;
            let updatedBuilds = 0;
            const buildSummaries = [];

            for (const result of results) {
              const build = result.heroBuild;
              if (!build) continue;

              const stats = gcBuildSearch.upsertBuild(build, {
                numFavorites: result.numFavorites,
                numWeeklyFavorites: result.numWeeklyFavorites,
                numDailyFavorites: result.numDailyFavorites,
                numIgnores: result.numIgnores,
                numReports: result.numReports,
                source: 'gc_task_search'
              });

              if (stats.inserted) newBuilds++;
              if (stats.updated) updatedBuilds++;

              buildSummaries.push({
                id: build.heroBuildId || build.hero_build_id,
                name: build.name,
                heroId: build.heroId || build.hero_id,
                authorId: build.authorAccountId || build.author_account_id,
                favorites: result.numFavorites,
                weeklyFavorites: result.numWeeklyFavorites
              });
            }

            log('info', 'GC_SEARCH_BUILDS: Completed', {
              totalResults: results.length,
              newBuilds,
              updatedBuilds
            });

            return {
              ok: true,
              data: {
                totalResults: results.length,
                newBuilds,
                updatedBuilds,
                builds: buildSummaries
              }
            };

          } catch (err) {
            log('error', 'GC_SEARCH_BUILDS: Failed', { error: err.message });
            return { ok: false, error: err.message };
          }
        })();
        isAsync = finalizeTaskRun(task, promise);
        break;
      }

      default:
        throw new Error(`Unsupported task type: ${task.type}`);
    }
  } catch (err) {
    log('error', 'Failed to process steam task', { error: err.message });
    if (task && task.id) completeTask(task.id, 'FAILED', { ok:false, error: err.message }, err.message);
  } finally {
    if (!isAsync) taskInProgress = false;
  }
}

setInterval(() => {
  try {
    // Stale RUNNING Tasks aufräumen (z.B. nach Bridge-Crash)
    const now = nowSeconds();
    const staleCutoff = now - STALE_TASK_TIMEOUT_S;
    const staleResult = failStaleTasksStmt.run(STALE_TASK_TIMEOUT_S, now, now, staleCutoff);
    if (staleResult.changes > 0) {
      log('warn', 'Cleaned up stale RUNNING tasks', { count: staleResult.changes, cutoff_age_s: STALE_TASK_TIMEOUT_S });
      taskInProgress = false; // Erlaubt neuen Task nach Cleanup
    }
    processNextTask();
  } catch (err) { log('error', 'Task polling loop failed', { error: err.message }); }
}, Math.max(500, TASK_POLL_INTERVAL_MS));

setInterval(() => {
  syncFriendsAndLinks('interval').catch((err) => {
    log('warn', 'Friend sync loop failed', { error: err && err.message ? err.message : String(err) });
  });
}, FRIEND_SYNC_INTERVAL_MS);


  return {
    finalizeTaskRun,
    processNextTask,
  };
};
