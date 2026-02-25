'use strict';

const path = require('path');
const protobuf = require('protobufjs');

/**
 * Build Publisher — hero build protobuf encoding + GC send
 * Context: { state, client, log, writeDeadlockGcTrace, getPersonaName,
 *            convertKeysToCamelCase, DEADLOCK_APP_ID, PROTO_MASK,
 *            GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD,
 *            GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD_RESPONSE,
 *            DEADLOCK_APP_IDS }
 */
module.exports = (ctx) => {
  const {
    state, client, log, writeDeadlockGcTrace, getPersonaName,
    convertKeysToCamelCase, DEADLOCK_APP_ID, PROTO_MASK,
    GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD,
  } = ctx;

  // ---------- App ID ----------
  const DEADLOCK_APP_IDS = ctx.DEADLOCK_APP_IDS || [DEADLOCK_APP_ID];

  function getWorkingAppId() {
    return DEADLOCK_APP_IDS.find(id => id > 0) || 1422450;
  }

  // ---------- Protobuf ----------
  const HERO_BUILD_PROTO_PATH = path.join(__dirname, '..', 'protos', 'hero_build.proto');
  let heroBuildRoot = null;
  let HeroBuildMsg = null;
  let UpdateHeroBuildMsg = null;
  let UpdateHeroBuildResponseMsg = null;

  async function loadHeroBuildProto() {
    if (heroBuildRoot) return;
    heroBuildRoot = await protobuf.load(HERO_BUILD_PROTO_PATH);
    HeroBuildMsg = heroBuildRoot.lookupType('CMsgHeroBuild');
    UpdateHeroBuildMsg = heroBuildRoot.lookupType('CMsgClientToGCUpdateHeroBuild');
    UpdateHeroBuildResponseMsg = heroBuildRoot.lookupType('CMsgClientToGCUpdateHeroBuildResponse');
  }

  function getUpdateHeroBuildResponseMsg() {
    return UpdateHeroBuildResponseMsg;
  }

  // ---------- Build Helpers ----------
  function safeNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
  }

  function safeJsonParse(value) {
    if (!value) return {};
    try { return JSON.parse(value); }
    catch (err) { throw new Error(`Invalid JSON payload: ${err.message}`); }
  }

  function cleanBuildDetails(details) {
    if (!details || typeof details !== 'object') return { mod_categories: [] };
    const clone = JSON.parse(JSON.stringify(details));

    if (!Array.isArray(clone.mod_categories)) {
      clone.mod_categories = [];
    } else {
      clone.mod_categories = clone.mod_categories.map((cat) => {
        const c = { ...cat };
        if (Array.isArray(c.mods)) {
          c.mods = c.mods.map((m) => {
            const mm = { ...m };
            Object.keys(mm).forEach((k) => { if (mm[k] === null) delete mm[k]; });
            return mm;
          });
        }
        Object.keys(c).forEach((k) => { if (c[k] === null) delete c[k]; });
        return c;
      });
    }

    if (clone.ability_order && Array.isArray(clone.ability_order.currency_changes)) {
      clone.ability_order.currency_changes = clone.ability_order.currency_changes.map((cc) => {
        const obj = { ...cc };
        Object.keys(obj).forEach((k) => { if (obj[k] === null) delete obj[k]; });
        return obj;
      });
    }
    return clone;
  }

  function composeBuildDescription(base, originId, authorName) {
    const parts = [];
    const desc = (base || '').trim();
    if (desc) parts.push(desc);
    parts.push('www.twitch.tv/earlysalty (deutsch)');
    parts.push('Deutsche Deadlock Community: discord.gg/XmnqMbUZ7Z');
    if (originId) parts.push(`Original Build ID: ${originId}`);
    if (authorName) parts.push(`Original Author: ${authorName}`);
    return parts.join('\n');
  }

  async function buildUpdateHeroBuild(row, meta = {}) {
    const tags = safeJsonParse(row.tags_json || '[]');
    const details = cleanBuildDetails(safeJsonParse(row.details_json || '{}'));
    const targetName = meta.target_name || row.name || '';
    const targetDescription = meta.target_description || row.description || '';
    const targetLanguage = safeNumber(meta.target_language) ?? safeNumber(row.language) ?? 0;
    const authorId = safeNumber(meta.author_account_id) ?? safeNumber(row.author_account_id);
    const nowTs = Math.floor(Date.now() / 1000);
    const originId = safeNumber(meta.origin_build_id) ?? safeNumber(row.origin_build_id) ?? safeNumber(row.hero_build_id);
    const originalAuthorName = await getPersonaName(safeNumber(row.author_account_id));

    return {
      hero_build_id: safeNumber(row.hero_build_id),
      hero_id: safeNumber(row.hero_id),
      author_account_id: authorId,
      origin_build_id: originId,
      last_updated_timestamp: nowTs,
      publish_timestamp: nowTs,
      name: targetName,
      description: composeBuildDescription(targetDescription, originId, originalAuthorName),
      language: targetLanguage,
      version: (safeNumber(row.version) || 1) + 1,
      tags: Array.isArray(tags) ? tags.map((t) => Number(t)) : [],
      details: details && typeof details === 'object' ? details : {},
    };
  }

  async function buildMinimalHeroBuild(row, meta = {}) {
    log('info', 'buildMinimalHeroBuild: FIXED VERSION v2 - Creating minimal build');
    const targetName = meta.target_name || row.name || '';
    const targetDescription = meta.target_description || row.description || '';
    const targetLanguage = safeNumber(meta.target_language) ?? 0;
    const authorId = safeNumber(meta.author_account_id) ?? safeNumber(row.author_account_id);
    const originalAuthorName = await getPersonaName(safeNumber(row.author_account_id));

    const result = {
      hero_id: safeNumber(row.hero_id),
      author_account_id: authorId,
      origin_build_id: undefined,
      last_updated_timestamp: undefined,
      name: targetName,
      description: composeBuildDescription(targetDescription, row.hero_build_id, originalAuthorName),
      language: targetLanguage,
      version: 1,
      tags: [],
      details: { mod_categories: [] },
      publish_timestamp: undefined,
    };
    log('info', 'buildMinimalHeroBuild: Result details', {
      detailsType: typeof result.details,
      modCategoriesIsArray: Array.isArray(result.details.mod_categories),
      modCategoriesLength: result.details.mod_categories.length,
    });
    return result;
  }

  async function mapHeroBuildFromRow(row, meta = {}) {
    if (!row) throw new Error('hero_build_sources row missing');
    const tags = safeJsonParse(row.tags_json || '[]');
    const details = cleanBuildDetails(safeJsonParse(row.details_json || '{}'));
    const targetName = meta.target_name || row.name || '';
    const targetDescription = meta.target_description || row.description || '';
    const targetLanguage = safeNumber(meta.target_language) ?? safeNumber(row.language) ?? 0;
    const publisherAccountId = safeNumber(meta.author_account_id) ?? safeNumber(row.author_account_id);
    const nowTs = Math.floor(Date.now() / 1000);
    const originalAuthorName = await getPersonaName(safeNumber(row.author_account_id));

    return {
      hero_id: safeNumber(row.hero_id),
      author_account_id: publisherAccountId,
      origin_build_id: safeNumber(meta.origin_build_id) ?? safeNumber(row.hero_build_id) ?? safeNumber(row.origin_build_id),
      last_updated_timestamp: nowTs,
      publish_timestamp: nowTs,
      name: targetName,
      description: composeBuildDescription(targetDescription, meta.origin_build_id ?? row.hero_build_id, originalAuthorName),
      language: targetLanguage,
      version: (safeNumber(row.version) || 0) + 1,
      tags: Array.isArray(tags) ? tags.map((t) => Number(t)) : [],
      details: details && typeof details === 'object' ? details : {},
    };
  }

  async function sendHeroBuildUpdate(heroBuild) {
    await loadHeroBuildProto();
    if (!heroBuild || typeof heroBuild !== 'object') throw new Error('heroBuild payload missing');

    log('info', 'sendHeroBuildUpdate: Creating message', { heroBuildKeys: Object.keys(heroBuild) });

    const cleanedHeroBuild = {};
    for (const key in heroBuild) {
      if (heroBuild[key] !== undefined) cleanedHeroBuild[key] = heroBuild[key];
    }

    log('info', 'sendHeroBuildUpdate: Cleaned heroBuild', {
      cleanedKeys: Object.keys(cleanedHeroBuild),
      removedKeys: Object.keys(heroBuild).filter(k => heroBuild[k] === undefined),
    });

    let heroBuildMsg, message;
    try {
      const camelCaseHeroBuild = convertKeysToCamelCase(cleanedHeroBuild);
      heroBuildMsg = HeroBuildMsg.create(camelCaseHeroBuild);
    } catch (err) {
      log('error', 'sendHeroBuildUpdate: HeroBuildMsg.create() failed', { error: err.message, stack: err.stack });
      throw new Error(`HeroBuildMsg.create failed: ${err.message}`);
    }

    try {
      message = UpdateHeroBuildMsg.create({ heroBuild: heroBuildMsg });
    } catch (err) {
      log('error', 'sendHeroBuildUpdate: UpdateHeroBuildMsg.create() failed', { error: err.message, stack: err.stack });
      throw new Error(`UpdateHeroBuildMsg.create failed: ${err.message}`);
    }

    let payload;
    try {
      payload = UpdateHeroBuildMsg.encode(message).finish();
    } catch (err) {
      log('error', 'sendHeroBuildUpdate: encode().finish() failed', { error: err.message, stack: err.stack });
      throw new Error(`Protobuf encoding failed: ${err.message}`);
    }

    if (!payload || !Buffer.isBuffer(payload)) {
      throw new Error(`Invalid payload after encoding: type=${typeof payload}`);
    }
    if (payload.length === 0) {
      throw new Error('Encoded payload is empty - this indicates a protobuf encoding issue');
    }

    return new Promise((resolve, reject) => {
      if (state.heroBuildPublishWaiter) {
        reject(new Error('Another hero build publish is in flight'));
        return;
      }
      const timeout = setTimeout(() => {
        state.heroBuildPublishWaiter = null;
        reject(new Error('Timed out waiting for build publish response'));
      }, 20000);
      state.heroBuildPublishWaiter = {
        resolve: (resp) => { clearTimeout(timeout); state.heroBuildPublishWaiter = null; resolve(resp); },
        reject: (err) => { clearTimeout(timeout); state.heroBuildPublishWaiter = null; reject(err); },
      };
      writeDeadlockGcTrace('send_update_hero_build', {
        heroId: heroBuild.hero_id, language: heroBuild.language, name: heroBuild.name,
        mode: heroBuild.hero_build_id ? 'update' : 'new', version: heroBuild.version,
        origin_build_id: heroBuild.origin_build_id, author: heroBuild.author_account_id,
        payloadHex: payload.toString('hex'),
      });
      log('info', 'Sending UpdateHeroBuild', {
        payloadHex: payload.toString('hex').slice(0, 200), payloadLength: payload.length,
        heroId: heroBuild.hero_id, language: heroBuild.language, name: heroBuild.name,
        mode: heroBuild.hero_build_id ? 'update' : 'new', version: heroBuild.version,
      });
      client.sendToGC(DEADLOCK_APP_ID, PROTO_MASK | GC_MSG_CLIENT_TO_GC_UPDATE_HERO_BUILD, {}, payload);
    });
  }

  return {
    getWorkingAppId,
    loadHeroBuildProto,
    getUpdateHeroBuildResponseMsg,
    cleanBuildDetails,
    composeBuildDescription,
    buildUpdateHeroBuild,
    buildMinimalHeroBuild,
    mapHeroBuildFromRow,
    sendHeroBuildUpdate,
  };
};
