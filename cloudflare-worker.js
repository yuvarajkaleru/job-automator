/**
 * Job-bot relay — Cloudflare Worker (free plan), one Cron Trigger: "* * * * *" (every minute).
 *
 * Every minute it does ONE of these:
 *   1. At 8:00, 11:00, 14:00, 17:00, 20:00, 23:00 IST -> starts the GitHub "scrape" run (full scan, on
 *      time, unlike GitHub's own cron).
 *   2. At 9, 10, 12, 13, 15, 16, 18, 19, 21, 22 IST (on the hour) -> starts a "quick" run: LinkedIn /
 *      Naukri / Indeed top searches + all company feeds, silent unless it finds a new job. Skipped if a
 *      run is still busy. This is what makes you one of the first applicants.
 *   3. Otherwise -> peeks at Telegram. If you sent /scan or tapped a button, and no GitHub run is
 *      already busy, it starts an "approvals" run that handles it (≈1–2 min later you get the result).
 *      Nothing pending -> it does nothing, so it costs no GitHub minutes.
 *
 * It never reads or stores your messages beyond checking whether something is waiting.
 *
 * Settings -> Variables and Secrets:
 *   TELEGRAM_BOT_TOKEN  (secret)  same token as the GitHub secret
 *   CHAT_ID             (text)    your Telegram chat id
 *   GH_TOKEN            (secret)  fine-grained GitHub token: only job-automator, "Actions: Read and write"
 *   GH_REPO             (text)    yuvarajkaleru/job-automator
 */

const SCAN_HOURS_IST = [8, 11, 14, 17, 20, 23];                    // full scans
const QUICK_HOURS_IST = [9, 10, 12, 13, 15, 16, 18, 19, 21, 22];    // hourly quick scans in between
const WORKFLOW_FILE = "job_runner.yml";

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(tick(env));
  },
  // Opening the worker URL in a browser shows a simple health check.
  async fetch(request, env) {
    return new Response("job-bot relay is running", { headers: { "content-type": "text/plain" } });
  },
};

async function tick(env) {
  const ist = new Date(Date.now() + 5.5 * 3600 * 1000); // IST clock read through the UTC getters
  const hour = ist.getUTCHours();
  const minute = ist.getUTCMinutes();

  if (minute === 0 && SCAN_HOURS_IST.includes(hour)) {
    await dispatch(env, "scrape");
    return;
  }
  if (minute === 0 && QUICK_HOURS_IST.includes(hour)) {
    if (!(await runActive(env))) await dispatch(env, "quick", false); // busy -> the running scan covers it
    return;
  }

  // Peek at waiting Telegram updates. No offset = nothing is consumed; the GitHub run does that.
  let data;
  try {
    const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getUpdates?timeout=0&limit=20`);
    data = await r.json();
  } catch (e) {
    console.log("telegram peek failed", e);
    return;
  }
  if (!data.ok) return; // e.g. 409 while a GitHub run is listening — that run is handling the taps
  const mine = (data.result || []).filter((u) => {
    const chat = u.message?.chat?.id ?? u.callback_query?.message?.chat?.id ?? u.callback_query?.from?.id;
    return String(chat) === String(env.CHAT_ID);
  });
  if (!mine.length) return;
  if (await runActive(env)) return; // a run is already going; it (or the next tick) will pick these up
  if (await recentFailure(env)) return; // last run crashed <10 min ago: back off instead of looping

  const ok = await dispatch(env, "approvals", minute % 15 === 0); // warn at most every 15 min
  if (!ok) return;

  // Instant feedback so you know it was received.
  for (const u of mine) {
    if (u.callback_query) {
      await tg(env, "answerCallbackQuery", {
        callback_query_id: u.callback_query.id,
        text: "⏳ Got it — working on it (about 1–3 min)",
      });
    } else if ((u.message?.text || "").toLowerCase().startsWith("/scan")) {
      await tg(env, "sendMessage", { chat_id: env.CHAT_ID, text: "⏳ Got it — starting a scan now." });
    }
  }
}

async function runActive(env) {
  for (const status of ["in_progress", "queued"]) {
    const r = await gh(env, `/actions/runs?status=${status}&per_page=1`, "GET");
    if (!r.ok) continue;
    const j = await r.json();
    if ((j.total_count || 0) > 0) return true;
  }
  return false;
}

async function recentFailure(env) {
  const r = await gh(env, `/actions/runs?status=completed&per_page=1`, "GET");
  if (!r.ok) return false;
  const run = (await r.json()).workflow_runs?.[0];
  if (!run || run.conclusion !== "failure") return false;
  return Date.now() - Date.parse(run.updated_at) < 10 * 60 * 1000;
}

async function dispatch(env, mode, warn = true) {
  const r = await gh(env, `/actions/workflows/${WORKFLOW_FILE}/dispatches`, "POST", {
    ref: "main",
    inputs: { mode },
  });
  if (r.status >= 300) {
    const text = await r.text();
    console.log("dispatch failed", r.status, text);
    if (warn) await tg(env, "sendMessage", {
      chat_id: env.CHAT_ID,
      text: `⚠️ Relay could not start the GitHub run (${r.status}). Check GH_TOKEN / GH_REPO in the Cloudflare worker settings.`,
    });
    return false;
  }
  console.log("dispatched", mode);
  return true;
}

function gh(env, path, method, body) {
  return fetch(`https://api.github.com/repos/${env.GH_REPO}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${env.GH_TOKEN}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "job-bot-relay",
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
}

async function tg(env, method, payload) {
  try {
    await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.log("telegram call failed", method, e);
  }
}
