#!/usr/bin/env node

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { GoogleAuth } from "google-auth-library";
import { cert, getApps, initializeApp } from "firebase-admin/app";
import { FieldValue, getFirestore } from "firebase-admin/firestore";
import * as XLSX from "xlsx";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const OUTPUT_DIR = path.join(ROOT_DIR, "output");

const SOURCE = String(process.env.NAU_DB_ORIGEM || "nau").trim().toLowerCase() || "nau";
const TIME_ZONE = process.env.NAU_TIME_ZONE || process.env.TZ || "Europe/Lisbon";
const FORMACAO_COLLECTION = process.env.FORMACAO_COLLECTION || "formacao";
const LOGS_COLLECTION = process.env.NAU_LOGS_COLLECTION || "automacao_nau";
const DEFAULT_NORMALIZED_OUTPUT_PATH = path.join(OUTPUT_DIR, "formacaoNau.xlsx");
const COURSES_JSON_PATH = path.join(OUTPUT_DIR, "nau-cursos.json");
const SKIPPED_JSON_PATH = path.join(OUTPUT_DIR, "nau-links-skipados.json");
const FIRESTORE_BATCH_SIZE = parsePositiveInt(
  process.env.NAU_FIRESTORE_BATCH_SIZE,
  400,
);
const SKIP_FIRESTORE_UPLOAD = isTruthy(process.env.NAU_SKIP_FIRESTORE_UPLOAD);

function isTruthy(value) {
  return ["1", "true", "yes", "y", "on"].includes(
    String(value || "").trim().toLowerCase(),
  );
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function getDateParts(timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = formatter.formatToParts(new Date());
  return Object.fromEntries(
    parts
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
}

function getRunDateIso(timeZone) {
  const parts = getDateParts(timeZone);
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function getCapturedDate(timeZone) {
  const parts = getDateParts(timeZone);
  return `${parts.day}-${parts.month}-${parts.year}`;
}

function resolvePath(filePath) {
  if (!filePath) {
    return "";
  }

  return path.isAbsolute(filePath)
    ? filePath
    : path.resolve(ROOT_DIR, filePath);
}

function ensureFileExists(filePath, label) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`${label} nao encontrado: ${filePath}`);
  }
}

function readJson(filePath, label) {
  ensureFileExists(filePath, label);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readWorkbookObjects(filePath) {
  ensureFileExists(filePath, "Workbook normalizado");
  const workbook = XLSX.read(fs.readFileSync(filePath), { type: "buffer" });
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  return XLSX.utils.sheet_to_json(worksheet, {
    raw: false,
    defval: "",
  });
}

function normalizeFirestoreValue(value) {
  if (value === undefined) {
    return "";
  }

  if (typeof value === "number" && !Number.isFinite(value)) {
    return null;
  }

  return value;
}

function recordEvent(events, level, message, extra = undefined) {
  const levelUpper = String(level || "INFO").trim().toUpperCase();

  if (levelUpper === "ERROR") {
    console.error(message);
  } else if (levelUpper === "WARNING") {
    console.warn(message);
  } else {
    console.log(message);
  }

  const event = {
    ts: new Date().toISOString(),
    level: levelUpper,
    message,
  };

  if (extra !== undefined) {
    event.extra = extra;
  }

  events.push(event);
}

async function runNodeScript(scriptPath, envOverrides = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [scriptPath], {
      cwd: ROOT_DIR,
      env: {
        ...process.env,
        ...envOverrides,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      stdout += text;
      process.stdout.write(text);
    });

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      stderr += text;
      process.stderr.write(text);
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `Falha ao executar ${path.basename(scriptPath)} (exit=${code}).`,
          ),
        );
        return;
      }

      resolve({ stdout, stderr });
    });
  });
}

function resolveNormalizedOutputPath(stdout) {
  const match = String(stdout || "").match(/\[normalize\] Output: (.+)/);
  if (match?.[1]) {
    return match[1].trim();
  }

  return DEFAULT_NORMALIZED_OUTPUT_PATH;
}

function initializeFirebase() {
  if (getApps().length > 0) {
    return;
  }

  const serviceAccountJson = String(
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "",
  ).trim();
  const serviceAccountPath = resolvePath(
    String(process.env.FIREBASE_SERVICE_ACCOUNT_PATH || "").trim(),
  );

  if (serviceAccountJson) {
    initializeApp({
      credential: cert(JSON.parse(serviceAccountJson)),
    });
    return;
  }

  if (serviceAccountPath) {
    const payload = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    initializeApp({
      credential: cert(payload),
    });
    return;
  }

  initializeApp();
}

function getDb() {
  initializeFirebase();
  return getFirestore();
}

async function initRunLog(db, targetCollection) {
  const runDoc = db.collection(LOGS_COLLECTION).doc();
  await runDoc.set(
    {
      data_execucao: FieldValue.serverTimestamp(),
      status: "em_execucao",
      falha: "",
      firestore_target_collection: targetCollection,
      docs_written: 0,
      db_origem: SOURCE,
      events: [],
    },
    { merge: true },
  );
  return runDoc;
}

async function finalizeRunLog(runDoc, status, events, metrics, errorMessage = "") {
  if (!runDoc) {
    return;
  }

  await runDoc.set(
    {
      status: status === "success" ? "sucesso" : "falha",
      falha: errorMessage || "",
      events,
      finished_at: FieldValue.serverTimestamp(),
      ...metrics,
    },
    { merge: true },
  );
}

async function deleteRefsInBatches(db, refs) {
  let deleted = 0;

  for (let index = 0; index < refs.length; index += FIRESTORE_BATCH_SIZE) {
    const batch = db.batch();
    const slice = refs.slice(index, index + FIRESTORE_BATCH_SIZE);
    for (const ref of slice) {
      batch.delete(ref);
    }
    await batch.commit();
    deleted += slice.length;
  }

  return deleted;
}

async function cleanupNauDocs(db, activeCodes) {
  const snapshot = await db
    .collection(FORMACAO_COLLECTION)
    .where("db_origem", "==", SOURCE)
    .get();

  const activeCodeSet = new Set(
    activeCodes
      .map((value) => String(value || "").trim())
      .filter(Boolean),
  );

  const refsToDelete = [];
  let staleDeleted = 0;
  let duplicateDocIdDeleted = 0;

  for (const doc of snapshot.docs) {
    const data = doc.data() || {};
    const cod = String(data.cod || "").trim();
    const shouldKeep = activeCodeSet.has(doc.id);

    if (shouldKeep) {
      continue;
    }

    refsToDelete.push(doc.ref);

    if (cod && activeCodeSet.has(cod)) {
      duplicateDocIdDeleted += 1;
    } else {
      staleDeleted += 1;
    }
  }

  const totalDeleted = await deleteRefsInBatches(db, refsToDelete);

  return {
    cleanup_docs_seen: snapshot.size,
    cleanup_source_docs_seen: snapshot.size,
    cleanup_stale_deleted: staleDeleted,
    cleanup_duplicate_doc_id_deleted: duplicateDocIdDeleted,
    cleanup_total_deleted: totalDeleted,
    cleanup_indexed_active_courses: activeCodeSet.size,
  };
}

async function writeRowsToFirestore(rows, db) {
  const collection = db.collection(FORMACAO_COLLECTION);
  let batch = db.batch();
  let batchCount = 0;
  let written = 0;
  let skippedInvalidId = 0;
  let skippedDuplicateDocId = 0;
  const seenDocIds = new Set();

  for (const row of rows) {
    const docId = String(row.cod || "").trim();

    if (!docId) {
      skippedInvalidId += 1;
      continue;
    }

    if (seenDocIds.has(docId)) {
      skippedDuplicateDocId += 1;
      continue;
    }

    seenDocIds.add(docId);
    const payload = {};

    for (const [key, value] of Object.entries(row)) {
      payload[key] = normalizeFirestoreValue(value);
    }

    payload.cod = docId;
    batch.set(collection.doc(docId), payload);
    batchCount += 1;
    written += 1;

    if (batchCount >= FIRESTORE_BATCH_SIZE) {
      await batch.commit();
      batch = db.batch();
      batchCount = 0;
    }
  }

  if (batchCount > 0) {
    await batch.commit();
  }

  return {
    written,
    skipped_invalid_id: skippedInvalidId,
    skipped_duplicate_doc_id: skippedDuplicateDocId,
  };
}

async function getIdTokenHeaders(audience) {
  try {
    const auth = new GoogleAuth();
    const client = await auth.getIdTokenClient(audience);
    const requestHeaders = await client.getRequestHeaders(audience);

    if (typeof requestHeaders?.entries === "function") {
      return Object.fromEntries(requestHeaders.entries());
    }

    return { ...requestHeaders };
  } catch (error) {
    const gcloudCommand = process.platform === "win32" ? "gcloud.cmd" : "gcloud";

    return new Promise((resolve, reject) => {
      const child = spawn(gcloudCommand, [
        "auth",
        "print-identity-token",
        `--audiences=${audience}`,
      ]);

      let stdout = "";
      let stderr = "";

      child.stdout.on("data", (chunk) => {
        stdout += chunk.toString();
      });

      child.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });

      child.on("error", () => {
        reject(error);
      });

      child.on("close", (code) => {
        if (code !== 0) {
          reject(
            new Error(
              `gcloud auth print-identity-token falhou (exit=${code}): ${stderr.trim()}`,
            ),
          );
          return;
        }

        const token = stdout.trim();
        if (!token) {
          reject(new Error("Identity token vazio no fallback do gcloud."));
          return;
        }

        resolve({
          Authorization: `Bearer ${token}`,
        });
      });
    });
  }
}

async function triggerAmpAutomation(source, runId, payloadExtra = undefined) {
  const failOnError =
    process.env.AMP_AUTOMATION_FAIL_ON_ERROR === undefined
      ? true
      : isTruthy(process.env.AMP_AUTOMATION_FAIL_ON_ERROR);
  const url = String(process.env.AMP_AUTOMATION_URL || "").trim();

  if (!url) {
    const message = "AMP_AUTOMATION_URL nao configurada.";
    if (failOnError) {
      throw new Error(message);
    }

    return { status: "skipped", reason: message };
  }

  const ampSource =
    String(process.env.AMP_AUTOMATION_SOURCE || source).trim().toLowerCase() ||
    source;
  const audience = String(process.env.AMP_AUTOMATION_AUDIENCE || url).trim() || url;
  const timeoutSec = parsePositiveInt(
    process.env.AMP_AUTOMATION_TIMEOUT_SEC,
    3600,
  );
  const useIdToken =
    process.env.AMP_AUTOMATION_USE_ID_TOKEN === undefined
      ? true
      : isTruthy(process.env.AMP_AUTOMATION_USE_ID_TOKEN);
  const automationToken = String(process.env.AMP_AUTOMATION_TOKEN || "").trim();

  const payload = {
    source: ampSource,
    triggered_by: source,
    run_id: runId || "",
    ...(payloadExtra || {}),
  };

  const headers = {
    "Content-Type": "application/json",
  };

  if (automationToken) {
    headers["X-Automation-Token"] = automationToken;
  }

  if (useIdToken) {
    try {
      Object.assign(headers, await getIdTokenHeaders(audience));
    } catch (error) {
      if (failOnError) {
        throw new Error(
          `Falha ao obter ID token para o trigger da AMP: ${error.message}`,
        );
      }

      return {
        status: "error",
        reason: "id_token_error",
        message: error.message,
      };
    }
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(timeoutSec * 1000),
  });
  const responseText = (await response.text()).replace(/\s+/g, " ").trim().slice(0, 500);

  if (!response.ok && failOnError) {
    throw new Error(
      `AMP trigger falhou com status=${response.status} body=${responseText}`,
    );
  }

  return {
    status: response.ok ? "success" : "error",
    status_code: response.status,
    source: ampSource,
    url,
    response_excerpt: responseText,
  };
}

async function main() {
  const events = [];
  const stageDurations = {};
  const validationErrors = [];
  const validationWarnings = [];
  const startTs = Date.now();
  const runDateIso = getRunDateIso(TIME_ZONE);
  const capturedDate = getCapturedDate(TIME_ZONE);

  let runDoc = null;
  let runId = "";
  let currentStage = "init";
  let status = "success";
  let errorMessage = "";
  let ampTriggerResult = { status: "not_attempted" };

  const metrics = {
    run_id: "",
    db_origem: SOURCE,
    captured_date: capturedDate,
    run_date_iso: runDateIso,
    firestore_target_collection: FORMACAO_COLLECTION,
    links_total: 0,
    total_disponiveis: 0,
    total_skipados: 0,
    docs_written: 0,
    records_written: 0,
    records_skipped_invalid_id: 0,
    records_skipped_duplicate_doc_id: 0,
    output_xlsx_path: "",
    cleanup_docs_seen: 0,
    cleanup_source_docs_seen: 0,
    cleanup_stale_deleted: 0,
    cleanup_duplicate_doc_id_deleted: 0,
    cleanup_total_deleted: 0,
    cleanup_indexed_active_courses: 0,
    skipped_expired: 0,
    skipped_invalid_availability: 0,
  };

  try {
    let db = null;

    if (!SKIP_FIRESTORE_UPLOAD) {
      db = getDb();
      runDoc = await initRunLog(db, FORMACAO_COLLECTION);
      runId = runDoc.id;
      metrics.run_id = runId;
    }

    currentStage = "scrape";
    let stageStart = Date.now();
    recordEvent(events, "INFO", "Step 1: scraping NAU and generating raw outputs");
    await runNodeScript(path.join(ROOT_DIR, "scripts", "scrape-nau.js"));
    stageDurations.scrape = Number(((Date.now() - stageStart) / 1000).toFixed(2));

    currentStage = "normalize";
    stageStart = Date.now();
    recordEvent(
      events,
      "INFO",
      `Step 2: normalizing NAU output with obs=${capturedDate}`,
    );
    const normalizeRun = await runNodeScript(
      path.join(ROOT_DIR, "scripts", "normalize-nau.js"),
      {
        NAU_OBS_DATE: capturedDate,
      },
    );
    stageDurations.normalize = Number(
      ((Date.now() - stageStart) / 1000).toFixed(2),
    );

    const normalizedOutputPath = resolveNormalizedOutputPath(normalizeRun.stdout);
    metrics.output_xlsx_path = normalizedOutputPath;

    const coursesPayload = readJson(COURSES_JSON_PATH, "JSON de cursos");
    const skippedPayload = readJson(SKIPPED_JSON_PATH, "JSON de skipados");
    const normalizedRows = readWorkbookObjects(normalizedOutputPath);

    metrics.links_total = Number(coursesPayload.totalLinks || 0);
    metrics.total_disponiveis = Number(coursesPayload.totalDisponiveis || 0);
    metrics.total_skipados = Number(coursesPayload.totalSkipados || 0);
    metrics.rows_final = normalizedRows.length;

    const skippedItems = Array.isArray(skippedPayload.linksSkipados)
      ? skippedPayload.linksSkipados
      : [];
    metrics.skipped_expired = skippedItems.filter(
      (item) => item.motivo === "Curso expirado para a data de execucao.",
    ).length;
    metrics.skipped_invalid_availability = skippedItems.filter(
      (item) => item.motivo === "Nao foi possivel validar o campo Disponivel ate.",
    ).length;

    if (normalizedRows.length !== metrics.total_disponiveis) {
      validationWarnings.push(
        `Contagem do XLSX normalizado diverge do JSON de cursos (xlsx=${normalizedRows.length} json=${metrics.total_disponiveis}).`,
      );
    }

    if (SKIP_FIRESTORE_UPLOAD) {
      ampTriggerResult = {
        status: "skipped",
        reason: "NAU_SKIP_FIRESTORE_UPLOAD=true",
      };
      recordEvent(
        events,
        "WARNING",
        "Firestore/log upload skipped by configuration",
        ampTriggerResult,
      );
      recordEvent(events, "INFO", "Run completed successfully");
      return;
    }

    const dbReady = db || getDb();
    const activeCodes = normalizedRows.map((row) => row.cod);

    currentStage = "cleanup_formacao";
    stageStart = Date.now();
    recordEvent(
      events,
      "INFO",
      "Step 3: removing stale NAU docs from Firestore",
    );
    const cleanupStats = await cleanupNauDocs(dbReady, activeCodes);
    stageDurations.cleanup_formacao = Number(
      ((Date.now() - stageStart) / 1000).toFixed(2),
    );
    Object.assign(metrics, cleanupStats);
    recordEvent(
      events,
      "INFO",
      `Cleanup done: deleted=${cleanupStats.cleanup_total_deleted} stale=${cleanupStats.cleanup_stale_deleted} duplicate_doc_id=${cleanupStats.cleanup_duplicate_doc_id_deleted}`,
      cleanupStats,
    );

    currentStage = "firestore_upload";
    stageStart = Date.now();
    recordEvent(
      events,
      "INFO",
      `Step 4: writing ${normalizedRows.length} NAU rows to Firestore`,
    );
    const writeStats = await writeRowsToFirestore(normalizedRows, dbReady);
    stageDurations.firestore_upload = Number(
      ((Date.now() - stageStart) / 1000).toFixed(2),
    );
    metrics.docs_written = writeStats.written;
    metrics.records_written = writeStats.written;
    metrics.records_skipped_invalid_id = writeStats.skipped_invalid_id;
    metrics.records_skipped_duplicate_doc_id =
      writeStats.skipped_duplicate_doc_id;
    recordEvent(
      events,
      "INFO",
      `Firestore write stats: written=${writeStats.written} skipped_invalid=${writeStats.skipped_invalid_id} skipped_duplicate=${writeStats.skipped_duplicate_doc_id}`,
      writeStats,
    );

    if (writeStats.written !== normalizedRows.length - writeStats.skipped_invalid_id - writeStats.skipped_duplicate_doc_id) {
      validationWarnings.push(
        "Quantidade escrita no Firestore difere da contagem esperada apos filtragem de ids.",
      );
    }

    if (
      process.env.TRIGGER_AMP_AFTER_RUN === undefined ||
      isTruthy(process.env.TRIGGER_AMP_AFTER_RUN)
    ) {
      currentStage = "trigger_amp";
      stageStart = Date.now();
      recordEvent(events, "INFO", "Step 5: triggering AMP automation");
      ampTriggerResult = await triggerAmpAutomation(SOURCE, runId, {
        docs_written: metrics.docs_written,
        rows_final: metrics.rows_final,
        captured_date: capturedDate,
        links_total: metrics.links_total,
      });
      stageDurations.trigger_amp = Number(
        ((Date.now() - stageStart) / 1000).toFixed(2),
      );
      recordEvent(
        events,
        "INFO",
        `AMP trigger result: ${ampTriggerResult.status}`,
        ampTriggerResult,
      );
    } else {
      ampTriggerResult = {
        status: "skipped",
        reason: "TRIGGER_AMP_AFTER_RUN=false",
      };
      recordEvent(
        events,
        "INFO",
        "AMP trigger skipped by configuration",
        ampTriggerResult,
      );
    }

    recordEvent(events, "INFO", "Run completed successfully");
  } catch (error) {
    status = "failed";
    errorMessage = error instanceof Error ? error.message : String(error);
    validationErrors.push(errorMessage);
    recordEvent(events, "ERROR", `Run failed: ${errorMessage}`);
    throw error;
  } finally {
    metrics.duracao_segundos = Number(
      ((Date.now() - startTs) / 1000).toFixed(2),
    );
    metrics.duracao_etapas = stageDurations;
    metrics.ultima_etapa = status === "success" ? "concluido" : currentStage;
    metrics.validation_ok = validationErrors.length === 0;
    metrics.validation_errors = validationErrors;
    metrics.validation_warnings = validationWarnings;
    metrics.amp_trigger = ampTriggerResult;

    await finalizeRunLog(runDoc, status, events, metrics, errorMessage);
  }
}

main().catch(() => {
  process.exitCode = 1;
});
