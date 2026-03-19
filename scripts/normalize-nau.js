#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import * as XLSX from "xlsx";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT_DIR, "data");
const OUTPUT_DIR = path.join(ROOT_DIR, "output");

const DEFAULT_SOURCE_JSON_PATH = path.join(OUTPUT_DIR, "nau-cursos.json");
const DEFAULT_TEMPLATE_HEADERS_JSON_PATH = path.join(
  DATA_DIR,
  "formacao-template-headers.json",
);
const DEFAULT_AREA_MAP_JSON_PATH = path.join(DATA_DIR, "nau-area-map.json");
const DEFAULT_OUTPUT_XLSX_PATH = path.join(OUTPUT_DIR, "formacaoNau.xlsx");
const DEFAULT_DB_ORIGEM = "nau";
const ACPD_DB_ORIGEM = "acpd";
const DEFAULT_MODALIDADE = "online";
const EXTRA_HEADERS = ["ccdr", "cim"];
const LOCATION_INSERT_AFTER = "local_distrito";

const LANGUAGE_CODE_MAP = new Map([
  ["ingles", "EN"],
  ["espanhol", "ES"],
  ["portugues", "PT"],
]);

function ensureFileExists(filePath, label) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`${label} nao encontrado: ${filePath}`);
  }
}

function readJsonFile(filePath, label) {
  ensureFileExists(filePath, label);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readJson(filePath) {
  return readJsonFile(filePath, "JSON de origem");
}

function readWorkbookRows(filePath) {
  ensureFileExists(filePath, "Workbook");
  const workbook = XLSX.read(fs.readFileSync(filePath), { type: "buffer" });
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  return XLSX.utils.sheet_to_json(worksheet, {
    header: 1,
    raw: false,
    defval: "",
  });
}

function readWorkbookObjects(filePath) {
  ensureFileExists(filePath, "Workbook");
  const workbook = XLSX.read(fs.readFileSync(filePath), { type: "buffer" });
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  return XLSX.utils.sheet_to_json(worksheet, {
    raw: false,
    defval: "",
  });
}

function withExtraHeaders(templateHeaders) {
  const headers = [...templateHeaders];
  const insertAfterIndex = headers.indexOf(LOCATION_INSERT_AFTER);
  const extrasToAdd = EXTRA_HEADERS.filter((header) => !headers.includes(header));

  if (extrasToAdd.length === 0) {
    return headers;
  }

  if (insertAfterIndex === -1) {
    return [...headers, ...extrasToAdd];
  }

  headers.splice(insertAfterIndex + 1, 0, ...extrasToAdd);
  return headers;
}

function normalizeLookupKey(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
}

function buildAreaMap(rows) {
  const map = new Map();
  const duplicates = new Set();

  for (const row of rows) {
    const area = String(row.areaConhecimento || "").trim();
    if (!area) {
      continue;
    }

    const key = normalizeLookupKey(area);
    if (map.has(key)) {
      duplicates.add(area);
    }

    map.set(key, {
      setor_maior: String(row.setor_maior || "").trim(),
      setor_espec: String(row.setor_espec || "").trim(),
    });
  }

  if (duplicates.size > 0) {
    console.warn(
      `[warn] Areas duplicadas no dicionario; ultima ocorrencia prevaleceu: ${[...duplicates].join(", ")}`,
    );
  }

  return map;
}

function normalizeIdioma(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  const parts = raw
    .split(/\s+e\s+/i)
    .map((part) => {
      const key = normalizeLookupKey(part);
      return LANGUAGE_CODE_MAP.get(key) || part.trim().toUpperCase();
    })
    .filter(Boolean);

  return [...new Set(parts)].join(", ");
}

function buildWorkbookFallbackPath(outputPath) {
  const parsed = path.parse(outputPath);
  const stamp = new Date().toISOString().replaceAll(":", "-").replaceAll(".", "-");
  return path.join(parsed.dir, `${parsed.name}-${stamp}${parsed.ext}`);
}

function saveWorkbook(workbook, outputPath) {
  try {
    XLSX.writeFile(workbook, outputPath);
    return outputPath;
  } catch (error) {
    if (error?.code !== "EBUSY") {
      throw error;
    }

    const fallbackPath = buildWorkbookFallbackPath(outputPath);
    XLSX.writeFile(workbook, fallbackPath);
    console.warn(
      `[warn] Output bloqueado em ${outputPath}; gravado em ${fallbackPath}`,
    );
    return fallbackPath;
  }
}

function createEmptyRow(headers) {
  return Object.fromEntries(headers.map((header) => [header, ""]));
}

function resolveCourseDbOrigem(course, options = {}) {
  const fallback =
    String(options.dbOrigem || DEFAULT_DB_ORIGEM).trim() || DEFAULT_DB_ORIGEM;
  const origemColeta = String(course.origemColeta || "").trim().toLowerCase();

  if (origemColeta === ACPD_DB_ORIGEM) {
    return ACPD_DB_ORIGEM;
  }

  if (origemColeta === DEFAULT_DB_ORIGEM) {
    return DEFAULT_DB_ORIGEM;
  }

  return fallback;
}

function buildNormalizedRow(course, headers, areaMap, options = {}) {
  const row = createEmptyRow(headers);
  const codigo = String(course.codigo || "").trim();
  const titulo = String(course.titulo || "").trim();
  const area = String(course.areaConhecimento || "").trim();
  const areaMatch = areaMap.get(normalizeLookupKey(area));
  const dbOrigem = resolveCourseDbOrigem(course, options);
  const modalidade =
    String(options.modalidade || DEFAULT_MODALIDADE).trim() || DEFAULT_MODALIDADE;

  row.cod = codigo ? `${codigo}_${dbOrigem}` : "";
  row.db_origem = dbOrigem;
  row.url = String(course.link || "").trim();
  row.curso_nome_original = titulo;
  row["curso_nome_original.1"] = titulo;
  row.modalidade = modalidade;
  row.duracao = String(course.duracao || "").trim();
  row.setor_espec = areaMatch?.setor_espec || "";
  row.setor_maior = areaMatch?.setor_maior || "";
  row.Idioma = normalizeIdioma(course.idioma);
  row.obs = String(options.obsDate || "").trim();
  row.ccdr = "";
  row.cim = "";

  return row;
}

function writeNormalizedWorkbook(rows, headers, outputPath) {
  const worksheetRows = [
    headers,
    ...rows.map((row) => headers.map((header) => row[header] ?? "")),
  ];
  const worksheet = XLSX.utils.aoa_to_sheet(worksheetRows);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, "formacaoNau");
  return saveWorkbook(workbook, outputPath);
}

function loadTemplateHeaders(options = {}) {
  const templateXlsxPath = String(options.templateXlsxPath || "").trim();
  if (templateXlsxPath) {
    const templateRows = readWorkbookRows(templateXlsxPath);
    return (templateRows[0] || []).map((value) => String(value || "").trim());
  }

  const templateHeaders = readJsonFile(
    options.templateHeadersJsonPath || DEFAULT_TEMPLATE_HEADERS_JSON_PATH,
    "JSON de headers",
  );
  return Array.isArray(templateHeaders)
    ? templateHeaders.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
}

function loadAreaMapRows(options = {}) {
  const areaMapXlsxPath = String(options.areaMapXlsxPath || "").trim();
  if (areaMapXlsxPath) {
    return readWorkbookObjects(areaMapXlsxPath);
  }

  const areaMapRows = readJsonFile(
    options.areaMapJsonPath || DEFAULT_AREA_MAP_JSON_PATH,
    "JSON de areas",
  );
  return Array.isArray(areaMapRows) ? areaMapRows : [];
}

export function runNormalization(options = {}) {
  const sourceJsonPath = options.sourceJsonPath || DEFAULT_SOURCE_JSON_PATH;
  const outputXlsxPath = options.outputXlsxPath || DEFAULT_OUTPUT_XLSX_PATH;
  const sourcePayload = readJson(sourceJsonPath);
  const courses = Array.isArray(sourcePayload.cursos) ? sourcePayload.cursos : [];
  const templateHeaders = loadTemplateHeaders(options);
  const outputHeaders = withExtraHeaders(templateHeaders);
  const areaMap = buildAreaMap(loadAreaMapRows(options));

  if (templateHeaders.length === 0) {
    throw new Error("Header vazio no template.");
  }

  const normalizedRows = courses.map((course) =>
    buildNormalizedRow(course, outputHeaders, areaMap, {
      dbOrigem: options.dbOrigem,
      modalidade: options.modalidade,
      obsDate: options.obsDate,
    }),
  );

  const missingAreaCourses = courses.filter(
    (course) => !String(course.areaConhecimento || "").trim(),
  );
  const unmappedAreaCourses = courses.filter((course) => {
    const area = String(course.areaConhecimento || "").trim();
    return area && !areaMap.has(normalizeLookupKey(area));
  });

  const savedPath = writeNormalizedWorkbook(
    normalizedRows,
    outputHeaders,
    outputXlsxPath,
  );

  return {
    courses,
    normalizedRows,
    outputHeaders,
    savedPath,
    missingAreaCourses,
    unmappedAreaCourses,
  };
}

function main() {
  const result = runNormalization({
    sourceJsonPath: process.env.NAU_SOURCE_JSON_PATH || DEFAULT_SOURCE_JSON_PATH,
    templateXlsxPath: process.env.NAU_TEMPLATE_XLSX_PATH || "",
    templateHeadersJsonPath:
      process.env.NAU_TEMPLATE_HEADERS_JSON_PATH || DEFAULT_TEMPLATE_HEADERS_JSON_PATH,
    areaMapXlsxPath: process.env.NAU_AREA_MAP_XLSX_PATH || "",
    areaMapJsonPath:
      process.env.NAU_AREA_MAP_JSON_PATH || DEFAULT_AREA_MAP_JSON_PATH,
    outputXlsxPath:
      process.env.NAU_NORMALIZED_OUTPUT_PATH || DEFAULT_OUTPUT_XLSX_PATH,
    dbOrigem: process.env.NAU_DB_ORIGEM || DEFAULT_DB_ORIGEM,
    modalidade: process.env.NAU_MODALIDADE || DEFAULT_MODALIDADE,
    obsDate: process.env.NAU_OBS_DATE || "",
  });

  console.log(`[normalize] Cursos normalizados: ${result.normalizedRows.length}`);
  console.log(`[normalize] Output: ${result.savedPath}`);
  if (result.missingAreaCourses.length > 0) {
    console.warn(
      `[warn] ${result.missingAreaCourses.length} curso(s) sem areaConhecimento; setor_maior/setor_espec ficaram em branco.`,
    );
  }
  if (result.unmappedAreaCourses.length > 0) {
    const uniqueAreas = [
      ...new Set(result.unmappedAreaCourses.map((course) => course.areaConhecimento)),
    ];
    console.warn(
      `[warn] ${result.unmappedAreaCourses.length} curso(s) com area fora do dicionario: ${uniqueAreas.join(", ")}`,
    );
  }
}

try {
  main();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
