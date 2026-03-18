#!/usr/bin/env node

import { spawn } from "node:child_process";
import { mkdir, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import * as cheerio from "cheerio";
import he from "he";
import { chromium } from "playwright";
import * as XLSX from "xlsx";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const OUTPUT_DIR = path.join(ROOT_DIR, "output");

const DEFAULT_LISTING_URL =
  "https://www.nau.edu.pt/pt/cursos/?limit=21&offset=0";
const FIRECRAWL_COMMAND =
  process.platform === "win32" ? "firecrawl.cmd" : "firecrawl";
const FIRECRAWL_MAX_BUFFER = 50 * 1024 * 1024;
const CONCURRENCY = 4;
const TIME_ZONE = process.env.TZ || "Europe/Lisbon";
const FORCE_DIRECT_FETCH = ["1", "true", "yes"].includes(
  (process.env.NAU_DIRECT_FETCH || "").trim().toLowerCase(),
);
let firecrawlUnavailable = FORCE_DIRECT_FETCH;
const COURSE_HEADERS = [
  "link",
  "titulo",
  "duracao",
  "idioma",
  "areaConhecimento",
  "codigo",
  "disponivelAte",
];
const AREA_HEADERS = ["areaConhecimento"];
const NON_AREA_BADGES = new Set([
  "Avançado",
  "Disponível em edX.org",
  "Especialista dos Conteúdos",
  "Intermédio",
  "Portugal Digital",
  "Principiante",
]);

function getListingUrl() {
  return process.argv[2] || DEFAULT_LISTING_URL;
}

function getTodayIsoInTimeZone(timeZone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());

  const values = Object.fromEntries(
    parts
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );

  return `${values.year}-${values.month}-${values.day}`;
}

function isoDateToPt(isoDate) {
  if (!isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    return null;
  }

  const [year, month, day] = isoDate.split("-");
  return `${day}/${month}/${year}`;
}

function ptDateToIso(ptDate) {
  if (!ptDate || !/^\d{2}\/\d{2}\/\d{4}$/.test(ptDate)) {
    return null;
  }

  const [day, month, year] = ptDate.split("/");
  return `${year}-${month}-${day}`;
}

function normalizeText(value) {
  const normalized = value.replace(/\s+/g, " ").trim();

  if (/[ÃÂâ]/.test(normalized)) {
    return Buffer.from(normalized, "latin1").toString("utf8");
  }

  return normalized;
}

function extractValueAfterLabel(text, label) {
  if (!text) {
    return null;
  }

  const normalized = normalizeText(text);
  const pattern = new RegExp(`^${label}\\s*:?\\s*(.+)$`, "i");
  const match = normalized.match(pattern);
  return match ? match[1].trim() : null;
}

function normalizeCourseUrl(href) {
  const url = new URL(href);
  url.hash = "";
  url.search = "";
  return url.toString();
}

function getListingLimit(listingUrl) {
  const url = new URL(listingUrl);
  const parsed = Number.parseInt(url.searchParams.get("limit") || "21", 10);

  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }

  return 21;
}

function extractTotalCourses(text) {
  const patterns = [
    /A mostrar de \d+ a \d+ de ([\d.]+) cursos/i,
    /([\d.]+) cursos correspondem/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);

    if (match) {
      return Number.parseInt(match[1].replace(/\./g, ""), 10);
    }
  }

  return null;
}

function buildWorkbookFallbackPath(outputPath) {
  const parsed = path.parse(outputPath);
  const stamp = new Date().toISOString().replaceAll(":", "-").replaceAll(".", "-");
  return path.join(parsed.dir, `${parsed.name}-${stamp}${parsed.ext}`);
}

function saveWorkbook(workbook, outputPath, label) {
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
      `[warn] ${label} bloqueado em ${outputPath}; gravado em ${fallbackPath}`,
    );
    return fallbackPath;
  }
}

function writeCoursesWorkbook(rows, outputPath) {
  const worksheetRows = [
    COURSE_HEADERS,
    ...rows.map((row) =>
      COURSE_HEADERS.map((header) => row[header] == null ? "" : row[header]),
    ),
  ];
  const worksheet = XLSX.utils.aoa_to_sheet(worksheetRows);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, "Cursos");
  return saveWorkbook(workbook, outputPath, "Workbook de cursos");
}

function writeAreasWorkbook(areas, outputPath) {
  const worksheetRows = [
    AREA_HEADERS,
    ...areas.map((areaConhecimento) => [areaConhecimento]),
  ];
  const worksheet = XLSX.utils.aoa_to_sheet(worksheetRows);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, "AreasConhecimento");
  return saveWorkbook(workbook, outputPath, "Workbook de areas");
}

function isValidAreaBadge(value) {
  return typeof value === "string" && value.trim() && !NON_AREA_BADGES.has(value.trim());
}

function collectUniqueAreas(items) {
  return [...new Set(
    items
      .flatMap((item) => item.areaConhecimentoTodas || [])
      .filter(isValidAreaBadge),
  )].sort((left, right) => left.localeCompare(right, "pt"));
}

function extractAreaBadges($) {
  return [...new Set(
    $(".category-badge__title")
      .toArray()
      .map((element) => normalizeText($(element).text()))
      .filter(isValidAreaBadge),
  )];
}

async function collectCourseLinks(listingUrl) {
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage();
    const limit = getListingLimit(listingUrl);
    await page.goto(listingUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });
    await page.waitForSelector('a[href*="/pt/curso/"]', { timeout: 30000 });

    const summaryText = await page.locator("body").innerText();
    const totalCourses = extractTotalCourses(summaryText);
    const totalPages = Math.max(
      1,
      Math.ceil((totalCourses || limit) / limit),
    );
    const links = new Set();
    const visitedPages = [];

    for (let pageIndex = 0; pageIndex < totalPages; pageIndex += 1) {
      const pageUrl = new URL(listingUrl);
      pageUrl.searchParams.set("limit", String(limit));
      pageUrl.searchParams.set("offset", String(pageIndex * limit));

      console.log(
        `A recolher pagina ${pageIndex + 1}/${totalPages}: ${pageUrl.toString()}`,
      );

      await page.goto(pageUrl.toString(), {
        waitUntil: "domcontentloaded",
        timeout: 60000,
      });
      await page.waitForSelector('a[href*="/pt/curso/"]', { timeout: 30000 });

      const pageLinks = await page.locator('a[href*="/pt/curso/"]').evaluateAll(
        (anchors) =>
          anchors
            .map((anchor) => anchor.href)
            .filter((href) => href.includes("/pt/curso/")),
      );

      for (const href of pageLinks) {
        links.add(normalizeCourseUrl(href));
      }

      visitedPages.push(pageUrl.toString());
    }

    return {
      links: [...links],
      totalCourses,
      totalPages,
      visitedPages,
      limit,
    };
  } finally {
    await browser.close();
  }
}

async function extractAvailabilityWithPlaywright(url) {
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage();
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });
    await page.waitForTimeout(3000);

    const bodyText = await page.locator("body").innerText();
    const match = bodyText.match(/Disponível até\s+(\d{2}\/\d{2}\/\d{4})/);

    return match ? ptDateToIso(match[1]) : null;
  } finally {
    await browser.close();
  }
}

async function runFirecrawlScrape(url) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      FIRECRAWL_COMMAND,
      ["scrape", url, "--format", "html,markdown", "--json"],
      {
        cwd: ROOT_DIR,
        shell: process.platform === "win32",
      },
    );

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();

      if (stdout.length > FIRECRAWL_MAX_BUFFER) {
        child.kill();
        reject(new Error("Saida do Firecrawl excedeu o buffer maximo."));
      }
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `Firecrawl terminou com codigo ${code} ao processar ${url}.\n${stderr}`,
          ),
        );
        return;
      }

      try {
        resolve(JSON.parse(stdout));
      } catch (error) {
        reject(
          new Error(
            `Nao foi possivel fazer parse do JSON devolvido pelo Firecrawl para ${url}.\n${error.message}`,
          ),
        );
      }
    });
  });
}

async function fetchCourseHtml(url) {
  const response = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/html,application/xhtml+xml",
    },
  });

  const html = await response.text();
  if (!response.ok) {
    throw new Error(
      `Nao foi possivel obter o HTML do curso (${response.status} ${response.statusText}) para ${url}.`,
    );
  }

  return { html };
}

async function runFirecrawlScrapeWithRetry(url, retries = 1) {
  if (firecrawlUnavailable) {
    return fetchCourseHtml(url);
  }

  let lastError;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await runFirecrawlScrape(url);
    } catch (error) {
      lastError = error;
      if (attempt === retries) {
        throw error;
      }
    }
  }

  const message = String(lastError?.message || "");
  const shouldFallbackToFetch =
    /Insufficient credits/i.test(message) ||
    /Firecrawl terminou com codigo/i.test(message) ||
    /firecrawl/i.test(message);

  if (shouldFallbackToFetch) {
    firecrawlUnavailable = true;
    console.warn(
      `[fallback] Firecrawl indisponivel; a continuar com fetch direto para ${url}`,
    );
    return fetchCourseHtml(url);
  }

  throw lastError;
}

function getCharacteristicValue($, label) {
  const item = $(".characteristics__term")
    .toArray()
    .map((element) => normalizeText($(element).text()))
    .find((text) => text.toLowerCase().startsWith(label.toLowerCase()));

  return extractValueAfterLabel(item, label);
}

function getCourseRunsPayload($) {
  const encoded = $(".richie-react--syllabus-course-runs-list").attr(
    "data-props",
  );

  if (!encoded) {
    return null;
  }

  return JSON.parse(he.decode(encoded));
}

function extractPrimaryAvailability(courseRunsPayload) {
  const primaryRun = courseRunsPayload?.courseRuns?.[0];

  if (!primaryRun?.end) {
    return null;
  }

  return primaryRun.end.slice(0, 10);
}

async function parseCourseFromHtml(html, url, runDateIso) {
  const $ = cheerio.load(html);

  const courseRunsPayload = getCourseRunsPayload($);
  const areaBadges = extractAreaBadges($);
  const primaryArea = areaBadges[0] || null;
  const title = normalizeText($(".subheader__title").first().text());
  const codeText = normalizeText($(".subheader__code").first().text());
  const code =
    codeText.match(/\.\s*(.+)$/)?.[1]?.trim() ||
    extractValueAfterLabel(codeText, "Cód.") ||
    codeText;
  const duration = getCharacteristicValue($, "Duração");
  const language = getCharacteristicValue($, "Idiomas");
  const availableUntilIso =
    extractPrimaryAvailability(courseRunsPayload) ||
    (await extractAvailabilityWithPlaywright(url));

  if (!availableUntilIso) {
    return {
      status: "skipped",
      item: {
        link: url,
        titulo: title || null,
        duracao: duration || null,
        idioma: language || null,
        areaConhecimento: primaryArea,
        areaConhecimentoTodas: areaBadges,
        codigo: code || null,
        motivo: "Nao foi possivel validar o campo Disponivel ate.",
      },
    };
  }

  if (availableUntilIso < runDateIso) {
    return {
      status: "skipped",
      item: {
        link: url,
        titulo: title || null,
        duracao: duration || null,
        idioma: language || null,
        areaConhecimento: primaryArea,
        areaConhecimentoTodas: areaBadges,
        codigo: code || null,
        disponivelAte: isoDateToPt(availableUntilIso),
        motivo: "Curso expirado para a data de execucao.",
      },
    };
  }

  return {
    status: "included",
    item: {
      link: url,
      titulo: title || null,
      duracao: duration || null,
      idioma: language || null,
      areaConhecimento: primaryArea,
      areaConhecimentoTodas: areaBadges,
      codigo: code || null,
      disponivelAte: isoDateToPt(availableUntilIso),
    },
  };
}

async function mapWithConcurrency(items, limit, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= items.length) {
        return;
      }

      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, Math.max(items.length, 1)) },
    () => worker(),
  );

  await Promise.all(workers);
  return results;
}

async function main() {
  const listingUrl = getListingUrl();
  const runDateIso = getTodayIsoInTimeZone(TIME_ZONE);

  await mkdir(OUTPUT_DIR, { recursive: true });

  console.log(`A recolher links com Playwright: ${listingUrl}`);
  const listingData = await collectCourseLinks(listingUrl);
  const { links, totalCourses, totalPages, visitedPages, limit } = listingData;
  console.log(
    `Links encontrados: ${links.length} em ${totalPages} pagina(s) visitada(s)`,
  );

  const results = await mapWithConcurrency(
    links,
    CONCURRENCY,
    async (url, index) => {
      console.log(`[${index + 1}/${links.length}] Firecrawl scrape: ${url}`);
      const payload = await runFirecrawlScrapeWithRetry(url, 1);
      return parseCourseFromHtml(payload.html, url, runDateIso);
    },
  );

  const included = results
    .filter((result) => result.status === "included")
    .map((result) => result.item);
  const skipped = results
    .filter((result) => result.status === "skipped")
    .map((result) => result.item);
  const uniqueAreas = collectUniqueAreas(results.map((result) => result.item));

  const linksPath = path.join(OUTPUT_DIR, "nau-links.json");
  const coursesJsonPath = path.join(OUTPUT_DIR, "nau-cursos.json");
  const coursesExcelPath = path.join(OUTPUT_DIR, "nau-cursos.xlsx");
  const uniqueAreasExcelPath = path.join(OUTPUT_DIR, "nau-areas-conhecimento.xlsx");
  const legacyCoursesCsvPath = path.join(OUTPUT_DIR, "nau-cursos.csv");
  const skippedPath = path.join(OUTPUT_DIR, "nau-links-skipados.json");

  await writeFile(
    linksPath,
    `${JSON.stringify(
      {
        fonte: listingUrl,
        executadoEm: new Date().toISOString(),
        limitePorPagina: limit,
        totalPaginasVisitadas: totalPages,
        totalCursosNaListagem: totalCourses,
        totalLinks: links.length,
        paginasVisitadas: visitedPages,
        links,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  await writeFile(
    coursesJsonPath,
    `${JSON.stringify(
      {
        fonte: listingUrl,
        dataExecucao: runDateIso,
        fusoHorario: TIME_ZONE,
        totalLinks: links.length,
        totalDisponiveis: included.length,
        totalSkipados: skipped.length,
        cursos: included,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  const savedCoursesExcelPath = writeCoursesWorkbook(included, coursesExcelPath);
  const savedUniqueAreasExcelPath = writeAreasWorkbook(uniqueAreas, uniqueAreasExcelPath);
  try {
    await unlink(legacyCoursesCsvPath);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
  await writeFile(
    skippedPath,
    `${JSON.stringify(
      {
        fonte: listingUrl,
        dataExecucao: runDateIso,
        totalSkipados: skipped.length,
        linksSkipados: skipped,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  console.log(`Concluido. Cursos disponiveis: ${included.length}`);
  console.log(`JSON: ${coursesJsonPath}`);
  console.log(`Excel: ${savedCoursesExcelPath}`);
  console.log(`Areas: ${savedUniqueAreasExcelPath}`);
  console.log(`Skipados: ${skippedPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
