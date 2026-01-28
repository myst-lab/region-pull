import readline from "node:readline";
import { Readable } from "node:stream";
import { DecompressStream } from "zstd-napi";

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const REGIONS_DUMP_URL = must("REGIONS_DUMP_URL");
const SUPABASE_REGIONS_UPSERT_URL = must("SUPABASE_REGIONS_UPSERT_URL");
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);
const LOG_EVERY = Number(process.env.LOG_EVERY || 20000);
const UPSERT_RETRIES = Number(process.env.UPSERT_RETRIES || 5);
const UPSERT_RETRY_MS = Number(process.env.UPSERT_RETRY_MS || 1500);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jsonHeaders() {
  const h = { "Content-Type": "application/json", "Accept": "application/json" };
  if (SYNC_API_TOKEN) h["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

function looksLikeZstdMagic(buf) {
  // ZSTD frame magic: 28 B5 2F FD
  return buf?.length >= 4 && buf[0] === 0x28 && buf[1] === 0xB5 && buf[2] === 0x2F && buf[3] === 0xFD;
}

async function upsertBatch(batch_id, regions, batch_index) {
  const payload = { batch_id, regions, batch_index };

  for (let attempt = 1; attempt <= UPSERT_RETRIES; attempt++) {
    const res = await fetch(SUPABASE_REGIONS_UPSERT_URL, {
      method: "POST",
      headers: jsonHeaders(),
      body: JSON.stringify(payload)
    });

    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = null; }

    if (res.ok && (!json || json.success !== false)) return json;

    const retryable = [408, 429, 500, 502, 503, 504].includes(res.status);
    const msg = `Upsert failed (${res.status}) attempt ${attempt}/${UPSERT_RETRIES}: ${text}`;

    if (!retryable || attempt === UPSERT_RETRIES) {
      throw new Error(msg);
    }

    console.warn("⚠️", msg);
    await sleep(UPSERT_RETRY_MS * attempt);
  }
}

async function run() {
  console.log("🚀 Starting REGIONS sync");
  const batch_id = `regions-${Date.now()}`;

  console.log("⬇️ Downloading regions dump (.zst) ...");
  const dumpRes = await fetch(REGIONS_DUMP_URL, {
    headers: { "Accept-Encoding": "identity" }
  });

  if (!dumpRes.ok) {
    const t = await dumpRes.text().catch(() => "");
    throw new Error(`Regions download failed (${dumpRes.status}): ${t}`);
  }

  // Read first chunk to verify ZSTD (avoid HTML error page)
  const reader = dumpRes.body.getReader();
  const first = await reader.read();
  if (first.done || !first.value) throw new Error("Regions dump returned empty body.");

  const firstBuf = Buffer.from(first.value);
  if (!looksLikeZstdMagic(firstBuf)) {
    const preview = firstBuf.toString("utf8", 0, Math.min(firstBuf.length, 300));
    throw new Error(
      "Downloaded data is not ZSTD (magic mismatch). Likely an error page.\n" +
      `Preview:\n${preview}`
    );
  }

  // Rebuild stream (first chunk + rest)
  const rebuiltWebStream = new ReadableStream({
    start(controller) {
      controller.enqueue(first.value);
      (async () => {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          controller.enqueue(value);
        }
        controller.close();
      })().catch((err) => controller.error(err));
    }
  });

  const compressedNode = Readable.fromWeb(rebuiltWebStream);

  console.log("🔓 Decompressing ZSTD (streaming) ...");
  const decompressed = compressedNode.pipe(new DecompressStream());
  decompressed.setEncoding("utf8");

  const rl = readline.createInterface({ input: decompressed, crlfDelay: Infinity });

  console.log("🧾 Parsing JSONL + uploading batches...");
  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      rl.pause();
      await upsertBatch(batch_id, batch, batchIndex);
      total += batch.length;
      batch = [];
      batchIndex++;
      if (total % LOG_EVERY === 0) console.log(`✅ Upserted ${total} regions`);
      rl.resume();
    }
  }

  if (batch.length) {
    await upsertBatch(batch_id, batch, batchIndex);
    total += batch.length;
  }

  console.log(`🎉 Done. Total regions processed: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
