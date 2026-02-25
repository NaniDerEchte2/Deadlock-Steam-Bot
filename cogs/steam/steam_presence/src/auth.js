'use strict';

/**
 * Auth — Steam login/logout/guard flow
 * Context: { state, runtimeState, client, log, nowSeconds,
 *            ACCOUNT_NAME, ACCOUNT_PASSWORD,
 *            clearReconnectTimer, scheduleStatePublish }
 */
module.exports = (ctx) => {
  const {
    state, runtimeState, client, log, nowSeconds,
    ACCOUNT_NAME, ACCOUNT_PASSWORD,
    clearReconnectTimer,
  } = ctx;

  function updateRefreshToken(token) {
    state.tokens.refreshToken = token ? String(token).trim() : '';
    runtimeState.refresh_token_present = Boolean(state.tokens.refreshToken);
    scheduleStatePublish({ reason: 'refresh_token' });
  }

  function updateMachineToken(token) {
    state.tokens.machineAuthToken = token ? String(token).trim() : '';
    runtimeState.machine_token_present = Boolean(state.tokens.machineAuthToken);
    scheduleStatePublish({ reason: 'machine_token' });
  }

  function guardTypeFromDomain(domain) {
    const norm = String(domain || '').toLowerCase();
    if (norm.includes('email') || norm.includes('@')) return 'email';
    if (norm.includes('two-factor') || norm.includes('authenticator') || norm.includes('mobile')) return 'totp';
    if (norm.includes('device')) return 'device';
    if (norm.includes('.')) return 'email';
    return 'unknown';
  }

  function buildLoginOptions(overrides = {}) {
    if (overrides.refreshToken) return { refreshToken: overrides.refreshToken };
    if (state.tokens.refreshToken && !overrides.forceAccountCredentials) return { refreshToken: state.tokens.refreshToken };
    const accountName = overrides.accountName ?? ACCOUNT_NAME;
    const password = overrides.password ?? ACCOUNT_PASSWORD;

    if (!accountName) throw new Error('Missing Steam account name');
    if (!password) throw new Error('Missing Steam account password');

    const options = { accountName, password };
    if (overrides.twoFactorCode) options.twoFactorCode = String(overrides.twoFactorCode);
    if (overrides.authCode) options.authCode = String(overrides.authCode);
    if (Object.prototype.hasOwnProperty.call(overrides, 'rememberPassword')) options.rememberPassword = Boolean(overrides.rememberPassword);
    if (overrides.machineAuthToken) options.machineAuthToken = String(overrides.machineAuthToken);
    else if (state.tokens.machineAuthToken) options.machineAuthToken = state.tokens.machineAuthToken;
    return options;
  }

  function initiateLogin(source, payload) {
    if (client.steamID && client.steamID.isValid()) {
      const steamId64 = typeof client.steamID.getSteamID64 === 'function' ? client.steamID.getSteamID64() : String(client.steamID);
      return { started: false, reason: 'already_logged_on', steam_id64: steamId64 };
    }
    if (state.loginInProgress) return { started: false, reason: 'login_in_progress' };

    const overrides = {};
    if (payload) {
      if (Object.prototype.hasOwnProperty.call(payload, 'use_refresh_token') && !payload.use_refresh_token) overrides.forceAccountCredentials = true;
      if (Object.prototype.hasOwnProperty.call(payload, 'force_credentials') && payload.force_credentials) overrides.forceAccountCredentials = true;
      if (payload.account_name) overrides.accountName = payload.account_name;
      if (payload.password) overrides.password = payload.password;
      if (payload.refresh_token) overrides.refreshToken = payload.refresh_token;
      if (payload.two_factor_code) overrides.twoFactorCode = payload.two_factor_code;
      if (payload.auth_code) overrides.authCode = payload.auth_code;
      if (Object.prototype.hasOwnProperty.call(payload, 'remember_password')) overrides.rememberPassword = Boolean(payload.remember_password);
      if (payload.machine_auth_token) overrides.machineAuthToken = payload.machine_auth_token;
    }

    const options = buildLoginOptions(overrides);
    if (options.accountName) runtimeState.account_name = options.accountName;

    state.loginInProgress = true;
    runtimeState.logging_in = true;
    runtimeState.last_login_attempt_at = nowSeconds();
    runtimeState.last_login_source = source;
    runtimeState.last_error = null;
    state.pendingGuard = null;
    runtimeState.guard_required = null;
    state.manualLogout = false;
    clearReconnectTimer();

    log('info', 'Initiating Steam login', { using_refresh_token: Boolean(options.refreshToken), source });
    try {
      client.logOn(options);
    } catch (err) {
      state.loginInProgress = false;
      runtimeState.logging_in = false;
      runtimeState.last_error = { message: err.message };
      scheduleStatePublish({ reason: 'login_error', source, message: err.message });
      throw err;
    }

    scheduleStatePublish({ reason: 'login_start', source });
    return { started: true, using_refresh_token: Boolean(options.refreshToken), source };
  }

  function handleGuardCodeTask(payload) {
    if (!state.pendingGuard || !state.pendingGuard.callback) throw new Error('No Steam Guard challenge is pending');
    const code = payload && payload.code ? String(payload.code).trim() : '';
    if (!code) throw new Error('Steam Guard code is required');

    const callback = state.pendingGuard.callback;
    const domain = state.pendingGuard.domain;
    state.pendingGuard = null;
    runtimeState.guard_required = null;
    runtimeState.last_guard_submission_at = nowSeconds();

    try {
      callback(code);
      log('info', 'Submitted Steam Guard code', { domain: domain || null });
    } catch (err) {
      throw new Error(`Failed to submit guard code: ${err.message}`);
    }

    scheduleStatePublish({ reason: 'guard_submit', domain: domain || null });
    return { accepted: true, domain: domain || null, type: guardTypeFromDomain(domain) };
  }

  function handleLogoutTask() {
    state.manualLogout = true;
    clearReconnectTimer();
    runtimeState.logging_in = false;
    state.loginInProgress = false;
    state.pendingGuard = null;
    runtimeState.guard_required = null;
    runtimeState.last_error = null;
    try { client.logOff(); } catch (err) { log('warn', 'logOff failed', { error: err.message }); }
    scheduleStatePublish({ reason: 'logout_command' });
    return { logged_off: true };
  }

  // scheduleStatePublish is late-bound (set after state.js is created)
  function scheduleStatePublish(context) {
    if (ctx.scheduleStatePublish) {
      try { ctx.scheduleStatePublish(context); } catch (err) { log('warn', 'State publish failed', { error: err.message }); }
    }
  }

  return {
    updateRefreshToken,
    updateMachineToken,
    guardTypeFromDomain,
    buildLoginOptions,
    initiateLogin,
    handleGuardCodeTask,
    handleLogoutTask,
  };
};
