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
const ACPD_SOURCE = "acpd";
const MANAGED_SOURCES = [...new Set([SOURCE, ACPD_SOURCE])];
const TIME_ZONE = process.env.NAU_TIME_ZONE || process.env.TZ || "Europe/Lisbon";
const FORMACAO_COLLECTION = process.env.FORMACAO_COLLECTION || "formacao";
const LOGS_COLLECTION = process.env.NAU_LOGS_COLLECTION || "automacao_nau";
const DEFAULT_NORMALIZED_OUTPUT_PATH = path.join(OUTPUT_DIR, "formacaoNau.xlsx");
const COURSES_JSON_PATH = path.join(OUTPUT_DIR, "nau-cursos.json");
const SKIPPED_JSON_PATH = path.join(OUTPUT_DIR, "nau-links-skipados.json");
const ACPD_COURSES_JSON_PATH = path.join(OUTPUT_DIR, "acpd-cursos.json");
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

function normalizeSourceValue(value) {
  return String(value || "").trim().toLowerCase();
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

function groupActiveDocIdsBySource(rows) {
  const activeDocIdsBySource = new Map();

  for (const row of rows) {
    const source = normalizeSourceValue(row.db_origem || SOURCE) || SOURCE;
    const docId = String(row.cod || "").trim();
    if (!docId) {
      continue;
    }

    if (!activeDocIdsBySource.has(source)) {
      activeDocIdsBySource.set(source, new Set());
    }

    activeDocIdsBySource.get(source).add(docId);
  }

  return activeDocIdsBySource;
}

function countRowsBySource(rows) {
  const counts = new Map();

  for (const row of rows) {
    const source = normalizeSourceValue(row.db_origem || SOURCE) || SOURCE;
    counts.set(source, (counts.get(source) || 0) + 1);
  }

  return counts;
}

function getLinksTotalForSource(source, metrics) {
  if (source === ACPD_SOURCE) {
    return Number(metrics.ACPD?.total_links || 0);
  }

  return Math.max(
    0,
    Number(metrics.links_total || 0) - Number(metrics.ACPD?.total_links || 0),
  );
}

async function cleanupManagedDocs(db, rows) {
  const activeDocIdsBySource = groupActiveDocIdsBySource(rows);
  const refsToDelete = [];
  let staleDeleted = 0;
  let duplicateDocIdDeleted = 0;
  let docsSeen = 0;

  for (const source of MANAGED_SOURCES) {
    const activeDocIds = activeDocIdsBySource.get(source) || new Set();
    const snapshot = await db
      .collection(FORMACAO_COLLECTION)
      .where("db_origem", "==", source)
      .get();

    docsSeen += snapshot.size;

    for (const doc of snapshot.docs) {
      const data = doc.data() || {};
      const cod = String(data.cod || "").trim();
      const shouldKeep = activeDocIds.has(doc.id);

      if (shouldKeep) {
        continue;
      }

      refsToDelete.push(doc.ref);

      if (cod && activeDocIds.has(cod)) {
        duplicateDocIdDeleted += 1;
      } else {
        staleDeleted += 1;
      }
    }
  }

  const totalDeleted = await deleteRefsInBatches(db, refsToDelete);

  return {
    cleanup_docs_seen: docsSeen,
    cleanup_source_docs_seen: docsSeen,
    cleanup_stale_deleted: staleDeleted,
    cleanup_duplicate_doc_id_deleted: duplicateDocIdDeleted,
    cleanup_total_deleted: totalDeleted,
    cleanup_indexed_active_courses: [...activeDocIdsBySource.values()].reduce(
      (sum, ids) => sum + ids.size,
      0,
    ),
    cleanup_sources: MANAGED_SOURCES,
  };
}

async function getExistingDocsById(rows, db) {
  const collection = db.collection(FORMACAO_COLLECTION);
  const docIds = [];
  const seenDocIds = new Set();

  for (const row of rows) {
    const docId = String(row.cod || "").trim();
    if (!docId || seenDocIds.has(docId)) {
      continue;
    }

    seenDocIds.add(docId);
    docIds.push(docId);
  }

  const existingDocs = new Map();

  for (let index = 0; index < docIds.length; index += FIRESTORE_BATCH_SIZE) {
    const slice = docIds.slice(index, index + FIRESTORE_BATCH_SIZE);
    const refs = slice.map((docId) => collection.doc(docId));
    const snapshots = await db.getAll(...refs);

    for (const snapshot of snapshots) {
      if (!snapshot.exists) {
        continue;
      }

      existingDocs.set(snapshot.id, snapshot.data() || {});
    }
  }

  return existingDocs;
}

async function writeRowsToFirestore(rows, db) {
  const collection = db.collection(FORMACAO_COLLECTION);
  const existingDocsById = await getExistingDocsById(rows, db);
  let batch = db.batch();
  let batchCount = 0;
  let written = 0;
  let skippedInvalidId = 0;
  let skippedDuplicateDocId = 0;
  let skippedExistingCod = 0;
  const seenDocIds = new Set();
  const writtenBySource = {};
  const skippedExistingCodBySource = {};

  for (const row of rows) {
    const docId = String(row.cod || "").trim();
    const rowSource = normalizeSourceValue(row.db_origem || SOURCE) || SOURCE;

    if (!docId) {
      skippedInvalidId += 1;
      continue;
    }

    if (seenDocIds.has(docId)) {
      skippedDuplicateDocId += 1;
      continue;
    }

    seenDocIds.add(docId);
    const existingDoc = existingDocsById.get(docId);
    const existingSource = normalizeSourceValue(existingDoc?.db_origem);

    if (existingDoc && existingSource && existingSource !== rowSource) {
      skippedExistingCod += 1;
      skippedExistingCodBySource[rowSource] =
        (skippedExistingCodBySource[rowSource] || 0) + 1;
      continue;
    }

    const payload = {};

    for (const [key, value] of Object.entries(row)) {
      payload[key] = normalizeFirestoreValue(value);
    }

    payload.cod = docId;
    batch.set(collection.doc(docId), payload);
    batchCount += 1;
    written += 1;
    writtenBySource[rowSource] = (writtenBySource[rowSource] || 0) + 1;

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
    skipped_existing_cod: skippedExistingCod,
    skipped_existing_cod_by_source: skippedExistingCodBySource,
    written_by_source: writtenBySource,
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
    normalizeSourceValue(source) ||
    normalizeSourceValue(process.env.AMP_AUTOMATION_SOURCE) ||
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
  let ampTriggerResult = { status: "not_attempted", sources: {} };

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
    records_skipped_existing_cod: 0,
    records_skipped_existing_cod_by_source: {},
    records_written_by_source: {},
    output_xlsx_path: "",
    cleanup_docs_seen: 0,
    cleanup_source_docs_seen: 0,
    cleanup_stale_deleted: 0,
    cleanup_duplicate_doc_id_deleted: 0,
    cleanup_total_deleted: 0,
    cleanup_indexed_active_courses: 0,
    skipped_expired: 0,
    skipped_invalid_availability: 0,
    ACPD: {
      total_links: 0,
      total_links_detalhe: 0,
      total_disponiveis_bruto: 0,
      total_disponiveis: 0,
      total_skipados: 0,
      duplicados_ignorados: 0,
      duplicados_internos_ignorados: 0,
    },
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
    const acpdCoursesPayload = readJson(
      ACPD_COURSES_JSON_PATH,
      "JSON de cursos ACPD",
    );
    const skippedPayload = readJson(SKIPPED_JSON_PATH, "JSON de skipados");
    const normalizedRows = readWorkbookObjects(normalizedOutputPath);

    metrics.links_total = Number(coursesPayload.totalLinks || 0);
    metrics.total_disponiveis = Number(coursesPayload.totalDisponiveis || 0);
    metrics.total_skipados = Number(coursesPayload.totalSkipados || 0);
    metrics.rows_final = normalizedRows.length;
    metrics.ACPD = {
      total_links: Number(acpdCoursesPayload.totalLinks || 0),
      total_links_detalhe: Number(acpdCoursesPayload.totalLinksDetalhe || 0),
      total_disponiveis_bruto: Number(
        acpdCoursesPayload.totalDisponiveisBruto || 0,
      ),
      total_disponiveis: Number(acpdCoursesPayload.totalDisponiveis || 0),
      total_skipados: Number(acpdCoursesPayload.totalSkipados || 0),
      duplicados_ignorados: Number(
        acpdCoursesPayload.totalDuplicadosIgnorados || 0,
      ),
      duplicados_internos_ignorados: Number(
        acpdCoursesPayload.totalDuplicadosInternosIgnorados || 0,
      ),
    };
    recordEvent(
      events,
      "INFO",
      `ACPD stats: links=${metrics.ACPD.total_links} bruto=${metrics.ACPD.total_disponiveis_bruto} final=${metrics.ACPD.total_disponiveis} duplicados_ignorados=${metrics.ACPD.duplicados_ignorados}`,
      metrics.ACPD,
    );

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

    if (
      metrics.ACPD.total_disponiveis >
      metrics.ACPD.total_disponiveis_bruto
    ) {
      validationWarnings.push(
        "Contagem final do ACPD ficou maior do que a contagem bruta antes da deduplicacao.",
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
    const rowsBySource = countRowsBySource(normalizedRows);

    currentStage = "cleanup_formacao";
    stageStart = Date.now();
    recordEvent(
      events,
      "INFO",
      `Step 3: removing stale docs from Firestore for sources ${MANAGED_SOURCES.join(", ")}`,
    );
    const cleanupStats = await cleanupManagedDocs(dbReady, normalizedRows);
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
    metrics.records_skipped_existing_cod = writeStats.skipped_existing_cod;
    metrics.records_skipped_existing_cod_by_source =
      writeStats.skipped_existing_cod_by_source;
    metrics.records_written_by_source = writeStats.written_by_source;
    recordEvent(
      events,
      "INFO",
      `Firestore write stats: written=${writeStats.written} skipped_invalid=${writeStats.skipped_invalid_id} skipped_duplicate=${writeStats.skipped_duplicate_doc_id} skipped_existing_cod=${writeStats.skipped_existing_cod}`,
      writeStats,
    );

    if (
      writeStats.written !==
      normalizedRows.length -
        writeStats.skipped_invalid_id -
        writeStats.skipped_duplicate_doc_id -
        writeStats.skipped_existing_cod
    ) {
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
      const ampSources = [...rowsBySource.keys()].filter(Boolean);
      recordEvent(
        events,
        "INFO",
        `Step 5: triggering AMP automation for sources ${ampSources.join(", ")}`,
      );
      const ampResults = {};

      for (const ampSource of ampSources) {
        const sourceRows = rowsBySource.get(ampSource) || 0;
        const sourceRowsWritten = writeStats.written_by_source?.[ampSource] || 0;
        ampResults[ampSource] = await triggerAmpAutomation(ampSource, runId, {
          docs_written: sourceRowsWritten,
          rows_final: sourceRows,
          captured_date: capturedDate,
          links_total: getLinksTotalForSource(ampSource, metrics),
        });
      }

      const ampStatuses = Object.values(ampResults).map((result) => result.status);
      ampTriggerResult = {
        status: ampStatuses.every((value) => value === "success")
          ? "success"
          : "partial_error",
        sources: ampResults,
      };
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
        sources: {},
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
