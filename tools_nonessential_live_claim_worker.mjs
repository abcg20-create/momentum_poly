import { ethers } from "ethers";
import { ClobClient, AssetType } from "@polymarket/clob-client";
import relayerPkg from "@polymarket/builder-relayer-client";
import proxyBuilderPkg from "@polymarket/builder-relayer-client/dist/builder/proxy.js";
import proxyEncodePkg from "@polymarket/builder-relayer-client/dist/encode/proxy.js";
import configPkg from "@polymarket/builder-relayer-client/dist/config/index.js";
import derivePkg from "@polymarket/builder-relayer-client/dist/builder/derive.js";

const { RelayClient, RelayerTxType } = relayerPkg;
const { buildProxyTransactionRequest } = proxyBuilderPkg;
const { encodeProxyTransactionData } = proxyEncodePkg;
const { getContractConfig } = configPkg;
const { deriveSafe, deriveProxyWallet } = derivePkg;

const CHAIN = { name: "matic", chainId: 137 };
const USDCe = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const CTF_ABI = [
  "function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets) external",
  "event PayoutRedemption(address indexed redeemer, address indexed collateralToken, bytes32 indexed parentCollectionId, bytes32 conditionId, uint256[] indexSets, uint256 payout)",
];
const DEFAULT_DATA_API_BASE = "https://data-api.polymarket.com";
const DEFAULT_GAMMA_API_BASE = "https://gamma-api.polymarket.com";
const DEFAULT_POLL_MS = 5000;
const DEFAULT_TARGET_SEC = 0;
const DEFAULT_WINDOW_SEC = 60;
const DEFAULT_MAX_PER_RUN = 24;
const DEFAULT_PRIORITY_FEE_GWEI = "30";
const DEFAULT_LOOKBACK_SLUGS = 5;
const DEFAULT_MANUAL_LOOKBACK_SLUGS = 25;
const DEFAULT_RECHECK_MS = 30000;
const FALLBACK_GAS_LIMIT = ethers.BigNumber.from(350000);
const DEFAULT_RELAYER_BASE = "https://relayer-v2.polymarket.com";
const DEFAULT_CLOB_BASE = "https://clob.polymarket.com";
const DEFAULT_BALANCE_CACHE_TTL_MS = 15000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeAddress(value) {
  const text = String(value || "").trim();
  return text ? text.toLowerCase() : "";
}

function makeProvider(url) {
  return new ethers.providers.StaticJsonRpcProvider(url, CHAIN);
}

function parseRpcUrls(env) {
  return String(env?.LIVE_CLAIM_RPC_URLS || env?.RPC_URLS || env?.RPC_URL || "")
    .split(",")
    .map((value) => String(value || "").trim())
    .filter(Boolean);
}

function resolveClaimProfileAddress(env, signerAddress) {
  return normalizeAddress(
    env?.LIVE_CLAIM_PROFILE_ADDRESS ||
    env?.POLY_FUNDER_ADDRESS ||
    env?.POLY_ADDRESS ||
    signerAddress ||
    ""
  );
}

function resolveRelayerApiKeyAddress(env, signerAddress) {
  return normalizeAddress(
    env?.RELAYER_API_KEY_ADDRESS ||
    signerAddress ||
    ""
  );
}

function parsePositiveNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function isFetchRetryableError(error) {
  const message = String(error?.message || error || "");
  const code = String(error?.code || "");
  return (
    code === "ECONNRESET" ||
    code === "ETIMEDOUT" ||
    code === "UND_ERR_CONNECT_TIMEOUT" ||
    message.includes("ECONNRESET") ||
    message.includes("ETIMEDOUT") ||
    message.includes("socket hang up") ||
    message.includes("fetch failed") ||
    message.includes("UND_ERR_CONNECT_TIMEOUT") ||
    message.includes("other side closed")
  );
}

function buildDisabledStatus(reason) {
  return {
    enabled: false,
    reason,
    running: false,
    startedAtMs: Date.now(),
    pollMs: DEFAULT_POLL_MS,
    targetSec: DEFAULT_TARGET_SEC,
    windowSec: DEFAULT_WINDOW_SEC,
    lookbackSlugs: DEFAULT_LOOKBACK_SLUGS,
    manualLookbackSlugs: DEFAULT_MANUAL_LOOKBACK_SLUGS,
    recheckMs: DEFAULT_RECHECK_MS,
    profileAddress: null,
    signerAddress: null,
    lastSeenSlug: null,
    lastEligibleSlug: null,
    lastCheckedSlug: null,
    lastCheckedAtMs: null,
    lastRedeemedSlug: null,
    lastRedeemedAtMs: null,
    lastRedeemedConditions: 0,
    lastRedeemedValueUsd: 0,
    lastClaimableRows: 0,
    lastClaimableValueUsd: 0,
    lastSkipReason: reason,
    lastError: reason,
  };
}

export function startLiveClaimWorker(opts = {}) {
  const env = opts.env || process.env;
  const enabled = String(env.LIVE_CLAIM_ENABLED || "0").trim() === "1";
  if (!enabled) {
    const disabled = buildDisabledStatus("LIVE_CLAIM_ENABLED=0");
    return {
      getStatus: () => ({ ...disabled }),
      stop() {},
      async runNow() {
        return { ok: false, skipped: true, reason: disabled.reason };
      },
    };
  }

  const privateKey = String(env.POLY_PRIVATE_KEY || "").trim();
  if (!privateKey) {
    const disabled = buildDisabledStatus("POLY_PRIVATE_KEY missing");
    return {
      getStatus: () => ({ ...disabled }),
      stop() {},
      async runNow() {
        return { ok: false, skipped: true, reason: disabled.reason };
      },
    };
  }

  const rpcUrls = parseRpcUrls(env);
  if (!rpcUrls.length) {
    const disabled = buildDisabledStatus("RPC_URLS/RPC_URL missing");
    return {
      getStatus: () => ({ ...disabled }),
      stop() {},
      async runNow() {
        return { ok: false, skipped: true, reason: disabled.reason };
      },
    };
  }

  const pollMs = Math.max(1000, Number(env.LIVE_CLAIM_POLL_MS || DEFAULT_POLL_MS));
  const targetSec = Math.max(0, Number(env.LIVE_CLAIM_TARGET_SEC || DEFAULT_TARGET_SEC));
  const windowSec = Math.max(1, Number(env.LIVE_CLAIM_WINDOW_SEC || DEFAULT_WINDOW_SEC));
  const maxPerRun = Math.max(1, Number(env.LIVE_CLAIM_MAX_PER_RUN || DEFAULT_MAX_PER_RUN));
  const lookbackSlugs = Math.max(1, Number(env.LIVE_CLAIM_LOOKBACK_SLUGS || DEFAULT_LOOKBACK_SLUGS));
  const manualLookbackSlugs = Math.max(
    lookbackSlugs,
    Number(env.LIVE_CLAIM_MANUAL_LOOKBACK_SLUGS || DEFAULT_MANUAL_LOOKBACK_SLUGS)
  );
  const recheckMs = Math.max(5000, Number(env.LIVE_CLAIM_RECHECK_MS || DEFAULT_RECHECK_MS));
  const minPriorityFeeGwei = String(env.LIVE_CLAIM_MIN_PRIORITY_FEE_GWEI || DEFAULT_PRIORITY_FEE_GWEI).trim() || DEFAULT_PRIORITY_FEE_GWEI;
  const dataApiBase = String(env.LIVE_CLAIM_DATA_API_BASE || DEFAULT_DATA_API_BASE).trim().replace(/\/+$/, "") || DEFAULT_DATA_API_BASE;
  const gammaApiBase = String(env.LIVE_CLAIM_GAMMA_API_BASE || DEFAULT_GAMMA_API_BASE).trim().replace(/\/+$/, "") || DEFAULT_GAMMA_API_BASE;
  const relayerBase = String(env.LIVE_CLAIM_RELAYER_BASE || env.RELAYER_BASE || DEFAULT_RELAYER_BASE).trim().replace(/\/+$/, "") || DEFAULT_RELAYER_BASE;
  const signer = new ethers.Wallet(privateKey);
  const profileAddress = resolveClaimProfileAddress(env, signer.address);
  const relayerApiKey = String(env.RELAYER_API_KEY || "").trim();
  const relayerApiKeyAddress = resolveRelayerApiKeyAddress(env, signer.address);
  const relayerContractConfig = getContractConfig(CHAIN.chainId);
  const derivedSafeAddress = normalizeAddress(deriveSafe(signer.address, relayerContractConfig.SafeContracts.SafeFactory));
  const derivedProxyAddress = normalizeAddress(deriveProxyWallet(signer.address, relayerContractConfig.ProxyContracts.ProxyFactory));
  const normalizedProfileAddress = normalizeAddress(profileAddress);
  const profileWalletType = normalizedProfileAddress === derivedSafeAddress
    ? "SAFE"
    : (normalizedProfileAddress === derivedProxyAddress ? "PROXY" : "EOA");
  const profileUsesSmartWallet = profileWalletType === "SAFE" || profileWalletType === "PROXY";
  if (profileUsesSmartWallet && !relayerApiKey) {
    const disabled = buildDisabledStatus("RELAYER_API_KEY missing for proxy-wallet claims");
    disabled.profileAddress = profileAddress || null;
    disabled.signerAddress = signer.address;
    return {
      getStatus: () => ({ ...disabled }),
      stop() {},
      async runNow() {
        return { ok: false, skipped: true, reason: disabled.reason };
      },
    };
  }
  const state = {
    enabled: true,
    reason: "ok",
    running: false,
    startedAtMs: Date.now(),
    pollMs,
    targetSec,
    windowSec,
    maxPerRun,
    lookbackSlugs,
    manualLookbackSlugs,
    recheckMs,
    profileAddress: profileAddress || null,
    signerAddress: signer.address,
    relayerEnabled: !!relayerApiKey,
    relayerApiKeyAddress: relayerApiKeyAddress || null,
    profileWalletType,
    lastSeenSlug: null,
    lastEligibleSlug: null,
    lastCheckedSlug: null,
    lastCheckedAtMs: null,
    lastRedeemedSlug: null,
    lastRedeemedAtMs: null,
    lastRedeemedConditions: 0,
    lastRedeemedValueUsd: 0,
    lastClaimableRows: 0,
    lastClaimableValueUsd: 0,
    lastAttemptSlug: null,
    lastAttemptAtMs: null,
    lastAttemptTrigger: null,
    lastCompletedSlug: null,
    lastCompletedAtMs: null,
    lastCompletedTrigger: null,
    lastCompletedResult: null,
    lastResult: null,
    lastSkipReason: null,
    lastError: null,
    lastKnownCollateralBalanceUsd: null,
    lastKnownCollateralBalanceAtMs: null,
    lastKnownOpenPositionsValueUsd: null,
    lastKnownPortfolioBalanceUsd: null,
    lastKnownPortfolioBalanceAtMs: null,
  };

  let rpcIndex = 0;
  let provider = makeProvider(rpcUrls[rpcIndex]);
  let wallet = new ethers.Wallet(privateKey, provider);
  let ctf = new ethers.Contract(CTF, CTF_ABI, wallet);
  let stopped = false;
  const payoutIface = new ethers.utils.Interface(CTF_ABI);
  let relayClient = null;
  let balanceClientPromise = null;
  let balanceRefreshPromise = null;
  let portfolioRefreshPromise = null;
  const balanceCacheTtlMs = Math.max(1000, Number(env.LIVE_CLAIM_BALANCE_CACHE_TTL_MS || DEFAULT_BALANCE_CACHE_TTL_MS));

  const log = typeof opts.log === "function" ? opts.log : (() => {});
  const onActivity = typeof opts.onActivity === "function" ? opts.onActivity : (() => {});
  const getSessionContext = typeof opts.getSessionContext === "function" ? opts.getSessionContext : null;
  const onClaimSettled = typeof opts.onClaimSettled === "function" ? opts.onClaimSettled : null;

  function rebuildClients() {
    wallet = new ethers.Wallet(privateKey, provider);
    ctf = new ethers.Contract(CTF, CTF_ABI, wallet);
    if (relayerApiKey) {
      relayClient = new RelayClient(
        relayerBase,
        CHAIN.chainId,
        wallet,
        undefined,
        profileWalletType === "PROXY" ? RelayerTxType.PROXY : RelayerTxType.SAFE
      );
      const baseSend = relayClient.send.bind(relayClient);
      relayClient.send = async (endpoint, method, options = {}) => {
        const headers = {
          ...(options?.headers || {}),
          RELAYER_API_KEY: relayerApiKey,
          RELAYER_API_KEY_ADDRESS: relayerApiKeyAddress,
        };
        return baseSend(endpoint, method, { ...options, headers });
      };
    } else {
      relayClient = null;
    }
  }
  rebuildClients();

  async function getBalanceClient() {
    if (balanceClientPromise) return balanceClientPromise;
    balanceClientPromise = (async () => {
      const clobBase = String(env.POLY_CLOB_BASE || env.CLOB_BASE || DEFAULT_CLOB_BASE).trim() || DEFAULT_CLOB_BASE;
      const signatureType = Number(env.POLY_SIGNATURE_TYPE ?? "2");
      const funder = String(
        env.POLY_FUNDER_ADDRESS ||
        env.POLY_ADDRESS ||
        signer.address
      ).trim() || signer.address;
      const apiKey = String(env.POLY_API_KEY || "").trim();
      const passphrase = String(env.POLY_PASSPHRASE || "").trim();
      const secret = String(env.POLY_SECRET || "").trim();
      let apiCreds = null;
      if (apiKey && passphrase && secret) {
        apiCreds = { key: apiKey, passphrase, secret };
      } else {
        const tempClient = new ClobClient(clobBase, CHAIN.chainId, signer);
        try {
          apiCreds = await tempClient.createOrDeriveApiKey();
        } catch {
          apiCreds = await tempClient.deriveApiKey();
        }
      }
      return new ClobClient(clobBase, CHAIN.chainId, signer, apiCreds, signatureType, funder);
    })().catch((error) => {
      balanceClientPromise = null;
      throw error;
    });
    return balanceClientPromise;
  }

  async function fetchCollateralBalanceUsd() {
    const client = await getBalanceClient();
    await client.updateBalanceAllowance({ asset_type: AssetType.COLLATERAL });
    const resp = await client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL });
    const rawBalance = Number(resp?.balance ?? NaN);
    if (!(Number.isFinite(rawBalance) && rawBalance >= 0)) return null;
    return Number((rawBalance / 1e6).toFixed(6));
  }

  async function resolveAuthoritativeCollateralBalance(forceFresh = false) {
    const cachedBalanceUsd = Number(state.lastKnownCollateralBalanceUsd);
    const cachedAtMs = Number(state.lastKnownCollateralBalanceAtMs);
    const cacheFresh =
      Number.isFinite(cachedBalanceUsd) &&
      Number.isFinite(cachedAtMs) &&
      cachedAtMs > 0 &&
      (Date.now() - cachedAtMs) <= balanceCacheTtlMs;
    if (!forceFresh && cacheFresh) return cachedBalanceUsd;
    if (balanceRefreshPromise) return balanceRefreshPromise;
    balanceRefreshPromise = (async () => {
      const freshBalanceUsd = await fetchCollateralBalanceUsd();
      if (Number.isFinite(Number(freshBalanceUsd)) && Number(freshBalanceUsd) >= 0) {
        state.lastKnownCollateralBalanceUsd = Number(Number(freshBalanceUsd).toFixed(6));
        state.lastKnownCollateralBalanceAtMs = Date.now();
      }
      return Number.isFinite(Number(freshBalanceUsd)) ? Number(freshBalanceUsd) : null;
    })().finally(() => {
      balanceRefreshPromise = null;
    });
    return balanceRefreshPromise;
  }

  async function fetchOpenPositionsCurrentValueUsd() {
    const userAddress = profileAddress || normalizeAddress(wallet.address);
    if (!userAddress) return 0;
    const pageLimit = 500;
    const maxPages = 20;
    let offset = 0;
    let totalCurrentValueUsd = 0;
    for (let page = 0; page < maxPages; page += 1) {
      const url = new URL("/positions", `${dataApiBase}/`);
      url.searchParams.set("user", userAddress);
      url.searchParams.set("sizeThreshold", "0");
      url.searchParams.set("limit", String(pageLimit));
      url.searchParams.set("offset", String(offset));
      const resp = await fetchWithRetry(url.toString(), {}, "portfolio-positions");
      if (!resp.ok) {
        const text = await resp.text().catch(() => "");
        throw new Error(`portfolio positions fetch failed ${resp.status}: ${text.slice(0, 300)}`);
      }
      const payload = await resp.json();
      const rows = Array.isArray(payload) ? payload : [];
      for (const row of rows) {
        if (!row || row.redeemable === true) continue;
        const currentValueUsd = parsePositiveNumber(row?.currentValue);
        const size = parsePositiveNumber(row?.size);
        if (!(currentValueUsd > 0 && size > 0)) continue;
        totalCurrentValueUsd += currentValueUsd;
      }
      if (rows.length < pageLimit) break;
      offset += rows.length;
    }
    return Number(totalCurrentValueUsd.toFixed(6));
  }

  async function resolveAuthoritativePortfolioBalance(forceFresh = false) {
    const cachedPortfolioUsd = Number(state.lastKnownPortfolioBalanceUsd);
    const cachedOpenValueUsd = Number(state.lastKnownOpenPositionsValueUsd);
    const cachedAtMs = Number(state.lastKnownPortfolioBalanceAtMs);
    const cacheFresh =
      Number.isFinite(cachedPortfolioUsd) &&
      Number.isFinite(cachedOpenValueUsd) &&
      Number.isFinite(cachedAtMs) &&
      cachedAtMs > 0 &&
      (Date.now() - cachedAtMs) <= balanceCacheTtlMs;
    if (!forceFresh && cacheFresh) {
      return {
        portfolioBalanceUsd: cachedPortfolioUsd,
        openPositionsValueUsd: cachedOpenValueUsd,
        cashBalanceUsd: Number.isFinite(Number(state.lastKnownCollateralBalanceUsd))
          ? Number(state.lastKnownCollateralBalanceUsd)
          : null,
        asOfMs: cachedAtMs,
        cached: true,
      };
    }
    if (portfolioRefreshPromise) return portfolioRefreshPromise;
    portfolioRefreshPromise = (async () => {
      const cashBalanceUsd = await resolveAuthoritativeCollateralBalance(forceFresh);
      const openPositionsValueUsd = await fetchOpenPositionsCurrentValueUsd();
      const portfolioBalanceUsd =
        Number.isFinite(Number(cashBalanceUsd)) && Number.isFinite(Number(openPositionsValueUsd))
          ? Number((Number(cashBalanceUsd) + Number(openPositionsValueUsd)).toFixed(6))
          : null;
      const asOfMs = Date.now();
      if (Number.isFinite(Number(openPositionsValueUsd)) && openPositionsValueUsd >= 0) {
        state.lastKnownOpenPositionsValueUsd = Number(Number(openPositionsValueUsd).toFixed(6));
      }
      if (Number.isFinite(Number(portfolioBalanceUsd)) && portfolioBalanceUsd >= 0) {
        state.lastKnownPortfolioBalanceUsd = Number(Number(portfolioBalanceUsd).toFixed(6));
        state.lastKnownPortfolioBalanceAtMs = asOfMs;
      }
      return {
        portfolioBalanceUsd: Number.isFinite(Number(portfolioBalanceUsd)) ? Number(portfolioBalanceUsd) : null,
        openPositionsValueUsd: Number.isFinite(Number(openPositionsValueUsd)) ? Number(openPositionsValueUsd) : null,
        cashBalanceUsd: Number.isFinite(Number(cashBalanceUsd)) ? Number(cashBalanceUsd) : null,
        asOfMs,
        cached: false,
      };
    })().finally(() => {
      portfolioRefreshPromise = null;
    });
    return portfolioRefreshPromise;
  }

  async function switchRpc() {
    for (let offset = 1; offset <= rpcUrls.length; offset += 1) {
      const nextIdx = (rpcIndex + offset) % rpcUrls.length;
      const nextUrl = rpcUrls[nextIdx];
      try {
        const nextProvider = makeProvider(nextUrl);
        await nextProvider.getBlockNumber();
        rpcIndex = nextIdx;
        provider = nextProvider;
        rebuildClients();
        log(`LIVE CLAIM switched RPC to ${nextUrl}`);
        return;
      } catch {}
    }
    throw new Error("all claim RPC endpoints failed");
  }

  function isNetworkError(error) {
    const message = String(error?.message || error || "");
    const code = String(error?.code || "");
    return (
      message.includes("could not detect network") ||
      message.includes("timeout") ||
      message.includes("missing response") ||
      message.includes("503 Service Temporarily Unavailable") ||
      message.includes("SERVER_ERROR") ||
      code === "NETWORK_ERROR"
      || code === "SERVER_ERROR"
    );
  }

  function isEstimateGasError(error) {
    const message = String(error?.message || error || "");
    const code = String(error?.code || "");
    return (
      code === "UNPREDICTABLE_GAS_LIMIT" ||
      message.includes("UNPREDICTABLE_GAS_LIMIT") ||
      message.includes("cannot estimate gas") ||
      message.includes("gas required exceeds allowance")
    );
  }

  async function fetchWithRetry(url, options = {}, label = "fetch") {
    let lastError = null;
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      try {
        return await fetch(url, options);
      } catch (error) {
        lastError = error;
        if (!isFetchRetryableError(error) || attempt >= 4) throw error;
        log(`LIVE CLAIM ${label} retry ${attempt} for ${url}: ${String(error?.message || error)}`);
        await sleep(250 * attempt);
      }
    }
    throw lastError || new Error(`${label} failed`);
  }

  async function resolveMarketForSlug(slug) {
    const safeSlug = String(slug || "").trim();
    if (!safeSlug) return null;
    const url = new URL(`/markets/slug/${encodeURIComponent(safeSlug)}`, `${gammaApiBase}/`);
    const resp = await fetchWithRetry(url.toString(), {}, "gamma-market");
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`gamma market lookup failed ${resp.status}: ${text.slice(0, 300)}`);
    }
    const payload = await resp.json();
    const conditionId = String(payload?.conditionId || "").trim();
    if (!conditionId) return null;
    return {
      slug: safeSlug,
      conditionId,
      closed: payload?.closed === true,
      acceptingOrders: payload?.acceptingOrders === true,
      umaResolutionStatus: String(payload?.umaResolutionStatus || "").trim().toLowerCase(),
    };
  }

  function isMarketRedeemReady(market) {
    if (!market?.conditionId) return false;
    if (market.closed !== true) return false;
    if (market.acceptingOrders === true) return false;
    return market.umaResolutionStatus === "resolved";
  }

  function shouldAllowRedeemFromPositionsFallback(market, rows) {
    if (!market?.conditionId) return false;
    if (!Array.isArray(rows) || !rows.length) return false;
    return rows.some((row) => parsePositiveNumber(row?.currentValue) > 0 && parsePositiveNumber(row?.size) > 0);
  }

  function buildRecentSlugWindow(context, count = lookbackSlugs) {
    const currentSlug = String(context?.currentSlug || "").trim();
    const sessionStartMs = Number(context?.sessionStartMs || 0);
    const marketIntervalMs = Number(context?.marketIntervalMs || 0);
    if (!(currentSlug && Number.isFinite(sessionStartMs) && sessionStartMs > 0 && Number.isFinite(marketIntervalMs) && marketIntervalMs > 0)) {
      return currentSlug ? [currentSlug] : [];
    }
    const base = currentSlug.replace(/-\d{10}$/i, "");
    const out = [];
    for (let i = 0; i < count; i += 1) {
      const startMs = sessionStartMs - (i * marketIntervalMs);
      if (!(Number.isFinite(startMs) && startMs > 0)) continue;
      out.push(`${base}-${Math.floor(startMs / 1000)}`);
    }
    return Array.from(new Set(out.filter(Boolean)));
  }

  function extractSlugStartSec(slug) {
    const text = String(slug || "").trim();
    if (!text) return 0;
    const match = text.match(/-(\d{10})$/);
    return match ? Number(match[1]) : 0;
  }

  function selectAutoRedeemableRows(context, rows, count = lookbackSlugs) {
    const currentSlug = String(context?.currentSlug || "").trim();
    const currentBase = currentSlug.replace(/-\d{10}$/i, "");
    const normalizedRows = Array.isArray(rows)
      ? rows.filter((row) => {
        const slug = String(row?.slug || "").trim();
        const conditionId = String(row?.conditionId || "").trim();
        if (!(slug && conditionId)) return false;
        if (currentBase && !slug.startsWith(`${currentBase}-`)) return false;
        return true;
      })
      : [];
    normalizedRows.sort((a, b) => {
      const slugDiff = extractSlugStartSec(String(b?.slug || "")) - extractSlugStartSec(String(a?.slug || ""));
      if (slugDiff) return slugDiff;
      return parsePositiveNumber(b?.currentValue) - parsePositiveNumber(a?.currentValue);
    });
    const allowedSlugs = new Set();
    for (const row of normalizedRows) {
      const slug = String(row?.slug || "").trim();
      if (!slug || allowedSlugs.has(slug)) continue;
      allowedSlugs.add(slug);
      if (allowedSlugs.size >= count) break;
    }
    return normalizedRows.filter((row) => allowedSlugs.has(String(row?.slug || "").trim()));
  }

  async function fetchRedeemablePositions(conditionId = null) {
    const userAddress = profileAddress || normalizeAddress(wallet.address);
    if (!userAddress) return [];
    const url = new URL("/positions", `${dataApiBase}/`);
    url.searchParams.set("user", userAddress);
    if (conditionId) url.searchParams.set("market", conditionId);
    url.searchParams.set("redeemable", "true");
    url.searchParams.set("sizeThreshold", "0");
    url.searchParams.set("limit", "500");
    url.searchParams.set("offset", "0");
    const resp = await fetchWithRetry(url.toString(), {}, "positions");
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`claim positions fetch failed ${resp.status}: ${text.slice(0, 300)}`);
    }
    const payload = await resp.json();
    const rows = Array.isArray(payload) ? payload : [];
    return rows.filter((row) => {
      if (!(row && row.redeemable && String(row.conditionId || "").trim())) return false;
      const currentValue = parsePositiveNumber(row?.currentValue);
      const size = parsePositiveNumber(row?.size);
      return currentValue > 0 && size > 0;
    });
  }

  async function buildFeeOverrides() {
    let fee;
    try {
      fee = await provider.getFeeData();
    } catch (error) {
      if (!isNetworkError(error)) throw error;
      await switchRpc();
      fee = await provider.getFeeData();
    }
    const maxPriorityFeePerGas = ethers.utils.parseUnits(minPriorityFeeGwei, "gwei");
    const suggestedMaxFee = fee.maxFeePerGas ?? ethers.utils.parseUnits("80", "gwei");
    const maxFeePerGas = suggestedMaxFee.mul(2).add(maxPriorityFeePerGas);
    return { maxPriorityFeePerGas, maxFeePerGas };
  }

  function parsePayoutRedemption(receipt, expectedConditionId) {
    const normalizedConditionId = String(expectedConditionId || "").trim().toLowerCase();
    for (const log of receipt?.logs || []) {
      if (String(log?.address || "").toLowerCase() !== String(CTF).toLowerCase()) continue;
      try {
        const parsed = payoutIface.parseLog(log);
        if (parsed?.name !== "PayoutRedemption") continue;
        const conditionId = String(parsed.args?.conditionId || "").trim().toLowerCase();
        if (normalizedConditionId && conditionId && conditionId !== normalizedConditionId) continue;
        return {
          redeemer: String(parsed.args?.redeemer || "").trim() || null,
          conditionId: String(parsed.args?.conditionId || "").trim() || null,
          payoutRaw: String(parsed.args?.payout?.toString?.() || "0"),
          indexSets: Array.isArray(parsed.args?.indexSets)
            ? parsed.args.indexSets.map((value) => String(value?.toString?.() || value))
            : [],
        };
      } catch {}
    }
    return null;
  }

  async function waitForRedeemReceipt(tx, conditionId) {
    const receipt = await tx.wait(1);
    const payout = parsePayoutRedemption(receipt, conditionId);
    return {
      txHash: tx.hash,
      receipt,
      payout,
      payoutRaw: String(payout?.payoutRaw || "0"),
    };
  }

  async function waitForRelayerReceipt(transactionId) {
    const startedAtMs = Date.now();
    while ((Date.now() - startedAtMs) < 180000) {
      const txns = await relayClient.getTransaction(transactionId);
      const txn = Array.isArray(txns) ? txns[0] : null;
      const stateText = String(txn?.state || "").trim();
      if (stateText === "STATE_CONFIRMED") return txn;
      if (stateText === "STATE_FAILED" || stateText === "STATE_INVALID") {
        throw new Error(`relayer transaction ${transactionId} ended in ${stateText}`);
      }
      await sleep(2000);
    }
    throw new Error(`relayer transaction ${transactionId} timed out`);
  }

  async function submitRelayerRequest(request) {
    const resp = await fetchWithRetry(`${relayerBase}/submit`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "RELAYER_API_KEY": relayerApiKey,
        "RELAYER_API_KEY_ADDRESS": relayerApiKeyAddress,
      },
      body: JSON.stringify(request),
    }, "relayer-submit");
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`relayer submit failed ${resp.status}: ${text.slice(0, 300)}`);
    }
    return resp.json();
  }

  async function redeemConditionViaRelayer(conditionId) {
    if (!relayClient) throw new Error("relayer client unavailable");
    const parentCollectionId = ethers.constants.HashZero;
    if (profileWalletType === "SAFE") {
      const data = ctf.interface.encodeFunctionData("redeemPositions", [USDCe, parentCollectionId, conditionId, [1, 2]]);
      const response = await relayClient.execute([{ to: CTF, data, value: "0" }], `Redeem ${conditionId}`);
      const relayerTxn = await waitForRelayerReceipt(String(response?.transactionID || "").trim());
      const txHash = String(relayerTxn?.transactionHash || "").trim();
      if (!txHash) throw new Error(`relayer transaction ${response?.transactionID || ""} confirmed without transaction hash`);
      const receipt = await provider.waitForTransaction(txHash, 1, 120000);
      const payout = parsePayoutRedemption(receipt, conditionId);
      return {
        txHash,
        receipt,
        payout,
        payoutRaw: String(payout?.payoutRaw || "0"),
        relayerTransactionId: String(response?.transactionID || "").trim() || null,
      };
    }
    if (profileWalletType !== "PROXY") {
      throw new Error(`unsupported profile wallet type for relayer redeem: ${profileWalletType}`);
    }
    const from = await wallet.getAddress();
    const relayPayload = await relayClient.getRelayPayload(from, "PROXY");
    const data = encodeProxyTransactionData([{
      to: CTF,
      typeCode: "1",
      data: ctf.interface.encodeFunctionData("redeemPositions", [USDCe, parentCollectionId, conditionId, [1, 2]]),
      value: "0",
    }]);
    const request = await buildProxyTransactionRequest(
      relayClient.signer,
      {
        from,
        gasPrice: "0",
        data,
        relay: relayPayload.address,
        nonce: relayPayload.nonce,
      },
      relayClient.contractConfig.ProxyContracts,
      `Redeem ${conditionId}`
    );
    request.proxyWallet = profileAddress;
    const response = await submitRelayerRequest(request);
    const relayerTxn = await waitForRelayerReceipt(String(response?.transactionID || "").trim());
    const txHash = String(relayerTxn?.transactionHash || "").trim();
    if (!txHash) throw new Error(`relayer transaction ${response?.transactionID || ""} confirmed without transaction hash`);
    const receipt = await provider.waitForTransaction(txHash, 1, 120000);
    const payout = parsePayoutRedemption(receipt, conditionId);
    return {
      txHash,
      receipt,
      payout,
      payoutRaw: String(payout?.payoutRaw || "0"),
      relayerTransactionId: String(response?.transactionID || "").trim() || null,
    };
  }

  async function redeemCondition(conditionId) {
    if (relayClient) {
      return redeemConditionViaRelayer(conditionId);
    }
    const parentCollectionId = ethers.constants.HashZero;
    const indexSets = [1, 2];
    const overrides = await buildFeeOverrides();
    try {
      const tx = await ctf.redeemPositions(USDCe, parentCollectionId, conditionId, indexSets, overrides);
      return waitForRedeemReceipt(tx, conditionId);
    } catch (error) {
      if (isEstimateGasError(error)) {
        const tx = await ctf.redeemPositions(USDCe, parentCollectionId, conditionId, indexSets, {
          ...overrides,
          gasLimit: FALLBACK_GAS_LIMIT,
        });
        return waitForRedeemReceipt(tx, conditionId);
      }
      if (!isNetworkError(error)) throw error;
      await switchRpc();
      const retryOverrides = await buildFeeOverrides();
      try {
        const tx = await ctf.redeemPositions(USDCe, parentCollectionId, conditionId, indexSets, retryOverrides);
        return waitForRedeemReceipt(tx, conditionId);
      } catch (retryError) {
        if (!isEstimateGasError(retryError)) throw retryError;
        const tx = await ctf.redeemPositions(USDCe, parentCollectionId, conditionId, indexSets, {
          ...retryOverrides,
          gasLimit: FALLBACK_GAS_LIMIT,
        });
        return waitForRedeemReceipt(tx, conditionId);
      }
    }
  }

  function eligibleForSession(context) {
    const sessionSlug = String(context?.currentSlug || "").trim();
    const sessionStartMs = Number(context?.sessionStartMs || 0);
    const nowMs = Number(context?.nowMs || Date.now());
    if (!(sessionSlug && Number.isFinite(sessionStartMs) && sessionStartMs > 0)) return false;
    const elapsedMs = Math.max(0, nowMs - sessionStartMs);
    const lowerBoundMs = targetSec * 1000;
    const upperBoundMs = lowerBoundMs + (windowSec * 1000);
    return elapsedMs >= lowerBoundMs && elapsedMs <= upperBoundMs;
  }

  async function tick(trigger = "timer") {
    if (stopped || state.running || !getSessionContext) {
      return { ok: false, skipped: true, reason: stopped ? "stopped" : (state.running ? "already_running" : "missing_getSessionContext") };
    }
    state.running = true;
    try {
      const context = await getSessionContext();
      const prevSeenSlug = String(state.lastSeenSlug || "").trim() || null;
      const currentSlug = String(context?.currentSlug || "").trim() || null;
      state.lastSeenSlug = currentSlug;
      const slugRefresh = !!(currentSlug && currentSlug !== prevSeenSlug);
      const forceManual = trigger === "manual-force";
      const bypassLiveEnabled = trigger !== "timer" && trigger !== "startup";
      if (!bypassLiveEnabled && !context?.liveEnabled) {
        state.lastSkipReason = "live_disabled";
        return { ok: true, skipped: true, reason: "live_disabled" };
      }
      if (!currentSlug) {
        state.lastSkipReason = "missing_slug";
        return { ok: true, skipped: true, reason: "missing_slug" };
      }
      const allowSessionRecheck = !forceManual && state.lastCheckedSlug === currentSlug;
      const bypassWindow = trigger === "manual" || forceManual || slugRefresh || allowSessionRecheck;
      if (!bypassWindow && !eligibleForSession(context)) {
        state.lastSkipReason = "outside_claim_window";
        return { ok: true, skipped: true, reason: "outside_claim_window", slug: currentSlug };
      }
      state.lastEligibleSlug = currentSlug;
      if (!forceManual && state.lastCheckedSlug === currentSlug) {
        const nowMs = Date.now();
        const lastCheckAgeMs = Math.max(0, nowMs - Number(state.lastCheckedAtMs || 0));
        const completedSameSlug = state.lastCompletedSlug === currentSlug;
        const settledResult = completedSameSlug ? String(state.lastCompletedResult || "").trim() : "";
        if (settledResult === "claimed") {
          state.lastSkipReason = "already_claimed_this_session";
          return { ok: true, skipped: true, reason: "already_claimed_this_session", slug: currentSlug };
        }
        if (lastCheckAgeMs < recheckMs) {
          state.lastSkipReason = "recently_checked_this_session";
          return { ok: true, skipped: true, reason: "recently_checked_this_session", slug: currentSlug, retryAfterMs: Math.max(0, recheckMs - lastCheckAgeMs) };
        }
      }

      state.lastSkipReason = null;
      state.lastCheckedSlug = currentSlug;
      state.lastCheckedAtMs = Date.now();
      state.lastAttemptSlug = currentSlug;
      state.lastAttemptAtMs = state.lastCheckedAtMs;
      state.lastAttemptTrigger = trigger;
      onActivity(`CHECKING LIVE CLAIMABLE TICKETS FOR ${currentSlug.toUpperCase()}`, {
        type: "live-claim-check-start",
        slug: currentSlug,
        trigger,
        slugRefresh,
        profileAddress,
      });

      const uniqueConditions = new Map();
      let totalClaimableRows = 0;
      if (forceManual) {
        const rows = await fetchRedeemablePositions(null);
        for (const row of rows) {
          const slug = String(row?.slug || "").trim();
          const conditionId = String(row?.conditionId || "").trim();
          if (!(slug && conditionId) || uniqueConditions.has(conditionId)) continue;
          const market = await resolveMarketForSlug(slug);
          const allowFallback = shouldAllowRedeemFromPositionsFallback(market, [row]);
          if (!isMarketRedeemReady(market) && !allowFallback) continue;
          if (!isMarketRedeemReady(market) && allowFallback) {
            onActivity(`ALLOWING LIVE CLAIM FROM POSITIONS FALLBACK FOR ${String(slug || "").toUpperCase()}`, {
              type: "live-claim-allow-positions-fallback",
              slug,
              trigger,
              conditionId,
              currentValue: parsePositiveNumber(row?.currentValue),
              size: parsePositiveNumber(row?.size),
              profileAddress,
              closed: market?.closed === true,
              acceptingOrders: market?.acceptingOrders === true,
              umaResolutionStatus: market?.umaResolutionStatus || null,
            });
          }
          totalClaimableRows += 1;
          uniqueConditions.set(conditionId, row);
        }
      } else {
        const redeemableRows = await fetchRedeemablePositions(null);
        const candidateRows = selectAutoRedeemableRows(context, redeemableRows, lookbackSlugs);
        const rowsBySlug = new Map();
        for (const row of candidateRows) {
          const slug = String(row?.slug || "").trim();
          if (!slug) continue;
          if (!rowsBySlug.has(slug)) rowsBySlug.set(slug, []);
          rowsBySlug.get(slug).push(row);
        }
        for (const [slug, rows] of rowsBySlug.entries()) {
          const market = await resolveMarketForSlug(slug);
          const allowFallback = shouldAllowRedeemFromPositionsFallback(market, rows);
          if (!isMarketRedeemReady(market) && !allowFallback) {
            onActivity(`SKIPPING LIVE CLAIM FOR UNRESOLVED MARKET ${String(slug || "").toUpperCase()}`, {
              type: "live-claim-skip-market-not-ready",
              slug,
              trigger,
              profileAddress,
              closed: market?.closed === true,
              acceptingOrders: market?.acceptingOrders === true,
              umaResolutionStatus: market?.umaResolutionStatus || null,
            });
            continue;
          }
          if (!isMarketRedeemReady(market) && allowFallback) {
            onActivity(`ALLOWING LIVE CLAIM FROM POSITIONS FALLBACK FOR ${String(slug || "").toUpperCase()}`, {
              type: "live-claim-allow-positions-fallback",
              slug,
              trigger,
              claimableRows: rows.length,
              claimableValueUsd: rows.reduce((sum, row) => sum + parsePositiveNumber(row?.currentValue), 0),
              profileAddress,
              closed: market?.closed === true,
              acceptingOrders: market?.acceptingOrders === true,
              umaResolutionStatus: market?.umaResolutionStatus || null,
            });
          }
          totalClaimableRows += rows.length;
          for (const row of rows) {
            const rowConditionId = String(row?.conditionId || "").trim();
            if (!rowConditionId || uniqueConditions.has(rowConditionId)) continue;
            uniqueConditions.set(rowConditionId, row);
          }
        }
      }
      state.lastClaimableRows = totalClaimableRows;
      const rows = Array.from(uniqueConditions.values());
      const claimableValueUsd = rows.reduce((sum, row) => sum + parsePositiveNumber(row?.currentValue), 0);
      state.lastClaimableValueUsd = claimableValueUsd;
      if (!rows.length) {
        state.lastRedeemedConditions = 0;
        state.lastRedeemedValueUsd = 0;
        state.lastCompletedResult = "no_claimables";
        state.lastResult = "no_claimables";
        state.lastSkipReason = null;
        state.lastError = null;
        state.lastCompletedSlug = currentSlug;
        state.lastCompletedAtMs = Date.now();
        state.lastCompletedTrigger = trigger;
        onActivity(`NO LIVE CLAIMABLE TICKETS FOR ${currentSlug.toUpperCase()}`, {
          type: "live-claim-none",
          slug: currentSlug,
          trigger,
          profileAddress,
        });
        return { ok: true, claimed: 0, claimableRows: 0, slug: currentSlug };
      }

      const batch = Array.from(uniqueConditions.entries()).slice(0, maxPerRun);
      let claimed = 0;
      let redeemedValueUsd = 0;
      let zeroPayoutCount = 0;
      for (const [conditionId, row] of batch) {
        const redemption = await redeemCondition(conditionId);
        const payoutRaw = String(redemption?.payoutRaw || "0");
        const payoutValue = Number(payoutRaw) / 1e6;
        const txHash = String(redemption?.txHash || "").trim() || null;
        const redeemer = String(redemption?.payout?.redeemer || "").trim() || wallet.address;
        if (!(Number.isFinite(payoutValue) && payoutValue > 0)) {
          zeroPayoutCount += 1;
          const mismatch = profileAddress && redeemer && normalizeAddress(profileAddress) !== normalizeAddress(redeemer);
          onActivity(`LIVE CLAIM TX CONFIRMED WITH ZERO PAYOUT FOR CONDITION ${conditionId}`, {
            type: "live-claim-zero-payout",
            slug: currentSlug,
            trigger,
            conditionId,
            txHash,
            title: String(row?.title || "").trim() || null,
            payoutRaw,
            payoutUsdc: payoutValue,
            redeemer,
            profileAddress,
            walletMismatch: mismatch,
          });
          state.lastError = mismatch
            ? `confirmed zero-payout claim from ${redeemer} while redeemable balance is on ${profileAddress}`
            : `confirmed zero-payout claim for ${conditionId}`;
          continue;
        }
        claimed += 1;
        redeemedValueUsd += payoutValue;
        onActivity(`CLAIMED LIVE TICKETS FOR CONDITION ${conditionId}`, {
          type: "live-claim-success",
          slug: currentSlug,
          trigger,
          conditionId,
          txHash,
          title: String(row?.title || "").trim() || null,
          payoutRaw,
          payoutUsdc: payoutValue,
          redeemer,
          profileAddress,
        });
        await sleep(300);
      }
      state.lastRedeemedSlug = currentSlug;
      state.lastRedeemedAtMs = Date.now();
      state.lastRedeemedConditions = claimed;
      state.lastRedeemedValueUsd = redeemedValueUsd;
      state.lastCompletedResult = claimed > 0
        ? "claimed"
        : (zeroPayoutCount > 0 ? "zero_payout" : "no_claimables");
      state.lastResult = state.lastCompletedResult;
      state.lastSkipReason = null;
      state.lastCompletedSlug = currentSlug;
      state.lastCompletedAtMs = Date.now();
      state.lastCompletedTrigger = trigger;
      if (claimed > 0 || zeroPayoutCount === 0) {
        state.lastError = null;
      }
      const authoritativePortfolio = claimed > 0
        ? await resolveAuthoritativePortfolioBalance(true).catch(() => null)
        : null;
      const collateralBalanceUsd = Number.isFinite(Number(authoritativePortfolio?.cashBalanceUsd))
        ? Number(authoritativePortfolio.cashBalanceUsd)
        : (claimed > 0 ? await resolveAuthoritativeCollateralBalance(true).catch(() => null) : null);
      const portfolioBalanceUsd = Number.isFinite(Number(authoritativePortfolio?.portfolioBalanceUsd))
        ? Number(authoritativePortfolio.portfolioBalanceUsd)
        : null;
      const openPositionsValueUsd = Number.isFinite(Number(authoritativePortfolio?.openPositionsValueUsd))
        ? Number(authoritativePortfolio.openPositionsValueUsd)
        : null;
      if (claimed > 0 && onClaimSettled) {
        try {
          await onClaimSettled({
            slug: currentSlug,
            trigger,
            claimed,
            redeemedValueUsd,
            claimableRows: rows.length,
            claimableValueUsd,
            collateralBalanceUsd,
            cashBalanceUsd: collateralBalanceUsd,
            portfolioBalanceUsd,
            openPositionsValueUsd,
            completedAtMs: Date.now(),
            profileAddress,
          });
        } catch (callbackError) {
          log(`LIVE CLAIM settlement callback failed: ${String(callbackError?.message || callbackError)}`);
        }
      }
      return {
        ok: true,
        claimed,
        redeemedValueUsd,
        claimableRows: rows.length,
        claimableValueUsd,
        uniqueConditions: uniqueConditions.size,
        zeroPayoutCount,
        slug: currentSlug,
        collateralBalanceUsd,
        portfolioBalanceUsd,
        openPositionsValueUsd,
      };
    } catch (error) {
      state.lastError = String(error?.message || error);
      state.lastCompletedResult = "error";
      state.lastResult = "error";
      state.lastSkipReason = null;
      if (state.lastCheckedSlug === state.lastSeenSlug && state.lastRedeemedSlug !== state.lastSeenSlug) {
        state.lastCheckedSlug = null;
        state.lastCheckedAtMs = null;
      }
      onActivity(`LIVE CLAIM FAILED`, {
        type: "live-claim-error",
        error: state.lastError,
        slug: state.lastSeenSlug,
        profileAddress,
      });
      return { ok: false, error: state.lastError };
    } finally {
      state.running = false;
    }
  }

  const handle = setInterval(() => {
    void tick("timer");
  }, pollMs);
  setTimeout(() => {
    void tick("startup");
  }, Math.min(2500, pollMs));

  onActivity(`LIVE CLAIM WORKER STARTED`, {
    type: "live-claim-start",
    profileAddress,
    targetSec,
    windowSec,
    pollMs,
    maxPerRun,
  });

  return {
    getStatus() {
      const lastCompletedResult = state.lastCompletedResult || null;
      const visibleLastResult = lastCompletedResult || state.lastResult || null;
      return {
        ...state,
        lastRedeemedValueUsd: Number.isFinite(Number(state.lastRedeemedValueUsd)) ? Number(state.lastRedeemedValueUsd) : 0,
        lastClaimableValueUsd: Number.isFinite(Number(state.lastClaimableValueUsd)) ? Number(state.lastClaimableValueUsd) : 0,
        lastKnownCollateralBalanceUsd: Number.isFinite(Number(state.lastKnownCollateralBalanceUsd))
          ? Number(state.lastKnownCollateralBalanceUsd)
          : null,
        lastKnownCollateralBalanceAtMs: Number.isFinite(Number(state.lastKnownCollateralBalanceAtMs))
          ? Number(state.lastKnownCollateralBalanceAtMs)
          : null,
        lastKnownOpenPositionsValueUsd: Number.isFinite(Number(state.lastKnownOpenPositionsValueUsd))
          ? Number(state.lastKnownOpenPositionsValueUsd)
          : null,
        lastKnownPortfolioBalanceUsd: Number.isFinite(Number(state.lastKnownPortfolioBalanceUsd))
          ? Number(state.lastKnownPortfolioBalanceUsd)
          : null,
        lastKnownPortfolioBalanceAtMs: Number.isFinite(Number(state.lastKnownPortfolioBalanceAtMs))
          ? Number(state.lastKnownPortfolioBalanceAtMs)
          : null,
        lastCompletedResult,
        lastResult: visibleLastResult,
        lastSkipReason: state.lastSkipReason || null,
      };
    },
    async getAuthoritativeBalance(opts = {}) {
      const forceFresh = opts && typeof opts === "object" && opts.forceFresh === true;
      const portfolio = await resolveAuthoritativePortfolioBalance(forceFresh);
      return {
        balanceUsd: Number.isFinite(Number(portfolio?.portfolioBalanceUsd)) ? Number(portfolio.portfolioBalanceUsd) : null,
        cashBalanceUsd: Number.isFinite(Number(portfolio?.cashBalanceUsd)) ? Number(portfolio.cashBalanceUsd) : null,
        openPositionsValueUsd: Number.isFinite(Number(portfolio?.openPositionsValueUsd)) ? Number(portfolio.openPositionsValueUsd) : null,
        asOfMs: Number.isFinite(Number(portfolio?.asOfMs)) ? Number(portfolio.asOfMs) : null,
        source: "worker_polymarket_portfolio_balance",
        cached: portfolio?.cached !== false,
      };
    },
    async runNow() {
      return tick("manual-force");
    },
    stop() {
      stopped = true;
      clearInterval(handle);
      onActivity(`LIVE CLAIM WORKER STOPPED`, {
        type: "live-claim-stop",
        profileAddress,
      });
    },
  };
}
