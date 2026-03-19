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

const DEFAULT_NAU_LISTING_URL =
  "https://www.nau.edu.pt/pt/cursos/?limit=21&offset=0";
const DEFAULT_ACPD_LISTING_URL =
  "https://academiaportugaldigital.pt/cursos?areaId=&competenceLevelId=&duration=&language=&partner=&isRecommended=&textSearch=";
const FIRECRAWL_COMMAND =
  process.platform === "win32" ? "firecrawl.cmd" : "firecrawl";
const FIRECRAWL_MAX_BUFFER = 50 * 1024 * 1024;
const CONCURRENCY = 4;
const TIME_ZONE = process.env.TZ || "Europe/Lisbon";
const FORCE_DIRECT_FETCH = ["1", "true", "yes"].includes(
  (process.env.NAU_DIRECT_FETCH || "").trim().toLowerCase(),
);
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
  "AvanÃ§ado",
  "Disponível em edX.org",
  "DisponÃ­vel em edX.org",
  "Especialista dos Conteúdos",
  "Especialista dos ConteÃºdos",
  "Intermédio",
  "IntermÃ©dio",
  "Portugal Digital",
  "Principiante",
]);
const SOURCE_NAU = "nau";
const SOURCE_ACPD = "acpd";

let firecrawlUnavailable = FORCE_DIRECT_FETCH;

function getNauListingUrl() {
  return process.argv[2] || DEFAULT_NAU_LISTING_URL;
}

function getAcpdListingUrl() {
  const envUrl = String(process.env.ACPD_LISTING_URL || "").trim();
  return envUrl || DEFAULT_ACPD_LISTING_URL;
}

function getAcpdSearchUrl(listingUrl) {
  const url = new URL(listingUrl);
  const searchUrl = new URL("/cursos-pesquisa", url.origin);
  searchUrl.search = url.search;
  return searchUrl.toString();
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
  const normalized = String(value || "").replace(/\s+/g, " ").trim();

  if (/[ÃƒÃ‚Ã¢]/.test(normalized)) {
    return Buffer.from(normalized, "latin1").toString("utf8");
  }

  return normalized;
}

function normalizeComparisonText(value) {
  return normalizeText(value).toLowerCase();
}

function normalizeCodeKey(value) {
  return normalizeText(value).replace(/\s+/g, "").toUpperCase();
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
      COURSE_HEADERS.map((header) => (row[header] == null ? "" : row[header])),
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
  return (
    typeof value === "string" &&
    value.trim() &&
    !NON_AREA_BADGES.has(value.trim())
  );
}

function collectUniqueAreas(items) {
  return [
    ...new Set(
      items
        .flatMap((item) => item.areaConhecimentoTodas || [])
        .filter(isValidAreaBadge),
    ),
  ].sort((left, right) => left.localeCompare(right, "pt"));
}

function extractAreaBadges($) {
  return [
    ...new Set(
      $(".category-badge__title")
        .toArray()
        .map((element) => normalizeText($(element).text()))
        .filter(isValidAreaBadge),
    ),
  ];
}

function extractOnclickLocationUrl(onclick, baseUrl) {
  const match = String(onclick || "").match(/location\.href='([^']+)'/i);
  return match?.[1] ? new URL(match[1], baseUrl).toString() : null;
}

function extractOnclickWindowOpenUrl(onclick, baseUrl) {
  const match = String(onclick || "").match(/window\.open\('([^']+)'/i);
  return match?.[1] ? new URL(match[1], baseUrl).toString() : null;
}

async function writeJsonFile(filePath, payload) {
  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function collectNauCourseLinks(listingUrl) {
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
    const totalPages = Math.max(1, Math.ceil((totalCourses || limit) / limit));
    const links = new Set();
    const visitedPages = [];

    for (let pageIndex = 0; pageIndex < totalPages; pageIndex += 1) {
      const pageUrl = new URL(listingUrl);
      pageUrl.searchParams.set("limit", String(limit));
      pageUrl.searchParams.set("offset", String(pageIndex * limit));

      console.log(
        `A recolher pagina NAU ${pageIndex + 1}/${totalPages}: ${pageUrl.toString()}`,
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
      listingUrl,
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

async function collectAcpdCourseLinks(listingUrl) {
  const searchUrl = getAcpdSearchUrl(listingUrl);
  const listingPayload = await fetchCourseHtml(searchUrl);
  const $ = cheerio.load(listingPayload.html);

  const detailLinks = [
    ...new Set(
      $("button")
        .toArray()
        .map((element) => ({
          text: normalizeText($(element).text()),
          onclick: $(element).attr("onclick") || "",
        }))
        .filter((button) => normalizeComparisonText(button.text) === "saber mais")
        .map((button) => extractOnclickLocationUrl(button.onclick, listingUrl))
        .filter(Boolean),
    ),
  ];

  const details = await mapWithConcurrency(
    detailLinks,
    CONCURRENCY,
    async (detailUrl, index) => {
      console.log(
        `[acpd ${index + 1}/${detailLinks.length}] detalhe: ${detailUrl}`,
      );
      const detailPayload = await fetchCourseHtml(detailUrl);
      const $detail = cheerio.load(detailPayload.html);
      const externalUrl = extractOnclickWindowOpenUrl(
        $detail("button")
          .toArray()
          .map((element) => ({
            text: normalizeText($detail(element).text()),
            onclick: $detail(element).attr("onclick") || "",
          }))
          .find(
            (button) =>
              normalizeComparisonText(button.text) === "ir para o curso",
          )?.onclick,
        detailUrl,
      );

      return {
        detailUrl,
        titulo: normalizeText($detail("h1 strong").last().text()) || null,
        externalUrl: externalUrl ? normalizeCourseUrl(externalUrl) : null,
      };
    },
  );

  const externalLinks = [
    ...new Set(details.map((detail) => detail.externalUrl).filter(Boolean)),
  ];
  const detailsWithoutExternalLink = details.filter((detail) => !detail.externalUrl);

  return {
    listingUrl,
    searchUrl,
    detailLinks,
    details,
    detailsWithoutExternalLink,
    links: externalLinks,
    totalCards: detailLinks.length,
  };
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
    const match = bodyText.match(/DisponÃ­vel atÃ©\s+(\d{2}\/\d{2}\/\d{4})/);

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
      `Nao foi possivel obter o HTML (${response.status} ${response.statusText}) para ${url}.`,
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
        break;
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

async function parseCourseFromHtml(html, url, runDateIso, source) {
  const $ = cheerio.load(html);

  const courseRunsPayload = getCourseRunsPayload($);
  const areaBadges = extractAreaBadges($);
  const primaryArea = areaBadges[0] || null;
  const title = normalizeText($(".subheader__title").first().text());
  const codeText = normalizeText($(".subheader__code").first().text());
  const code =
    codeText.match(/\.\s*(.+)$/)?.[1]?.trim() ||
    extractValueAfterLabel(codeText, "CÃ³d.") ||
    codeText;
  const duration = getCharacteristicValue($, "DuraÃ§Ã£o");
  const language = getCharacteristicValue($, "Idiomas");
  const availableUntilIso =
    extractPrimaryAvailability(courseRunsPayload) ||
    (await extractAvailabilityWithPlaywright(url));

  const baseItem = {
    link: url,
    titulo: title || null,
    duracao: duration || null,
    idioma: language || null,
    areaConhecimento: primaryArea,
    areaConhecimentoTodas: areaBadges,
    codigo: code || null,
    origemColeta: source,
  };

  if (!availableUntilIso) {
    return {
      status: "skipped",
      item: {
        ...baseItem,
        motivo: "Nao foi possivel validar o campo Disponivel ate.",
      },
    };
  }

  if (availableUntilIso < runDateIso) {
    return {
      status: "skipped",
      item: {
        ...baseItem,
        disponivelAte: isoDateToPt(availableUntilIso),
        motivo: "Curso expirado para a data de execucao.",
      },
    };
  }

  return {
    status: "included",
    item: {
      ...baseItem,
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

async function scrapeCourseUrls(links, runDateIso, source) {
  return mapWithConcurrency(links, CONCURRENCY, async (url, index) => {
    console.log(
      `[${source} ${index + 1}/${links.length}] scrape curso: ${url}`,
    );
    const payload = await runFirecrawlScrapeWithRetry(url, 1);
    return parseCourseFromHtml(payload.html, url, runDateIso, source);
  });
}

function splitScrapeResults(results) {
  return {
    included: results
      .filter((result) => result.status === "included")
      .map((result) => result.item),
    skipped: results
      .filter((result) => result.status === "skipped")
      .map((result) => result.item),
  };
}

function dedupeSecondaryCoursesByCode(primaryCourses, secondaryCourses) {
  const primaryCodeSet = new Set(
    primaryCourses
      .map((course) => normalizeCodeKey(course.codigo))
      .filter(Boolean),
  );
  const secondaryCodeSet = new Set();
  const kept = [];
  const duplicatesAgainstPrimary = [];
  const duplicatesWithinSecondary = [];

  for (const course of secondaryCourses) {
    const normalizedCode = normalizeCodeKey(course.codigo);

    if (!normalizedCode) {
      kept.push(course);
      continue;
    }

    if (primaryCodeSet.has(normalizedCode)) {
      duplicatesAgainstPrimary.push({
        ...course,
        motivo: "Codigo ja existe no output principal da NAU.",
      });
      continue;
    }

    if (secondaryCodeSet.has(normalizedCode)) {
      duplicatesWithinSecondary.push({
        ...course,
        motivo: "Codigo duplicado dentro do proprio output ACPD.",
      });
      continue;
    }

    secondaryCodeSet.add(normalizedCode);
    kept.push(course);
  }

  return {
    kept,
    duplicatesAgainstPrimary,
    duplicatesWithinSecondary,
  };
}

function buildCoursePayload({
  fonte,
  dataExecucao,
  fusoHorario,
  totalLinks,
  totalDisponiveis,
  totalSkipados,
  cursos,
  extra = {},
}) {
  return {
    fonte,
    dataExecucao,
    fusoHorario,
    totalLinks,
    totalDisponiveis,
    totalSkipados,
    ...extra,
    cursos,
  };
}

function buildSkippedPayload({
  fonte,
  dataExecucao,
  totalSkipados,
  linksSkipados,
  extra = {},
}) {
  return {
    fonte,
    dataExecucao,
    totalSkipados,
    ...extra,
    linksSkipados,
  };
}

async function main() {
  const nauListingUrl = getNauListingUrl();
  const acpdListingUrl = getAcpdListingUrl();
  const runDateIso = getTodayIsoInTimeZone(TIME_ZONE);

  await mkdir(OUTPUT_DIR, { recursive: true });

  console.log(`A recolher links com Playwright: ${nauListingUrl}`);
  const nauListingData = await collectNauCourseLinks(nauListingUrl);
  console.log(
    `Links NAU encontrados: ${nauListingData.links.length} em ${nauListingData.totalPages} pagina(s) visitada(s)`,
  );

  const nauResults = await scrapeCourseUrls(
    nauListingData.links,
    runDateIso,
    SOURCE_NAU,
  );
  const nauSplit = splitScrapeResults(nauResults);

  console.log(`A recolher links do ACPD: ${acpdListingUrl}`);
  const acpdListingData = await collectAcpdCourseLinks(acpdListingUrl);
  console.log(
    `Links ACPD encontrados: ${acpdListingData.links.length} (cards=${acpdListingData.totalCards})`,
  );

  const acpdResults = await scrapeCourseUrls(
    acpdListingData.links,
    runDateIso,
    SOURCE_ACPD,
  );
  const acpdSplitRaw = splitScrapeResults(acpdResults);
  const acpdDeduped = dedupeSecondaryCoursesByCode(
    nauSplit.included,
    acpdSplitRaw.included,
  );

  const acpdIncluded = acpdDeduped.kept;
  const combinedIncluded = [...nauSplit.included, ...acpdIncluded];
  const combinedSkipped = [...nauSplit.skipped, ...acpdSplitRaw.skipped];
  const uniqueAreas = collectUniqueAreas(combinedIncluded);

  const nauLinksPath = path.join(OUTPUT_DIR, "nau-links.json");
  const nauCoursesBaseJsonPath = path.join(OUTPUT_DIR, "nau-cursos-base.json");
  const nauCoursesBaseExcelPath = path.join(OUTPUT_DIR, "nau-cursos-base.xlsx");
  const nauBaseSkippedPath = path.join(OUTPUT_DIR, "nau-links-base-skipados.json");
  const acpdLinksPath = path.join(OUTPUT_DIR, "acpd-links.json");
  const acpdCoursesRawJsonPath = path.join(OUTPUT_DIR, "acpd-cursos-bruto.json");
  const acpdCoursesRawExcelPath = path.join(OUTPUT_DIR, "acpd-cursos-bruto.xlsx");
  const acpdCoursesJsonPath = path.join(OUTPUT_DIR, "acpd-cursos.json");
  const acpdCoursesExcelPath = path.join(OUTPUT_DIR, "acpd-cursos.xlsx");
  const acpdSkippedPath = path.join(OUTPUT_DIR, "acpd-links-skipados.json");
  const acpdDuplicatesPath = path.join(OUTPUT_DIR, "acpd-duplicados-ignorados.json");
  const combinedCoursesJsonPath = path.join(OUTPUT_DIR, "nau-cursos.json");
  const combinedCoursesExcelPath = path.join(OUTPUT_DIR, "nau-cursos.xlsx");
  const uniqueAreasExcelPath = path.join(OUTPUT_DIR, "nau-areas-conhecimento.xlsx");
  const legacyCoursesCsvPath = path.join(OUTPUT_DIR, "nau-cursos.csv");
  const combinedSkippedPath = path.join(OUTPUT_DIR, "nau-links-skipados.json");

  await writeJsonFile(nauLinksPath, {
    fonte: nauListingUrl,
    executadoEm: new Date().toISOString(),
    limitePorPagina: nauListingData.limit,
    totalPaginasVisitadas: nauListingData.totalPages,
    totalCursosNaListagem: nauListingData.totalCourses,
    totalLinks: nauListingData.links.length,
    paginasVisitadas: nauListingData.visitedPages,
    links: nauListingData.links,
  });

  await writeJsonFile(
    nauCoursesBaseJsonPath,
    buildCoursePayload({
      fonte: nauListingUrl,
      dataExecucao: runDateIso,
      fusoHorario: TIME_ZONE,
      totalLinks: nauListingData.links.length,
      totalDisponiveis: nauSplit.included.length,
      totalSkipados: nauSplit.skipped.length,
      cursos: nauSplit.included,
    }),
  );

  const savedNauCoursesBaseExcelPath = writeCoursesWorkbook(
    nauSplit.included,
    nauCoursesBaseExcelPath,
  );

  await writeJsonFile(
    nauBaseSkippedPath,
    buildSkippedPayload({
      fonte: nauListingUrl,
      dataExecucao: runDateIso,
      totalSkipados: nauSplit.skipped.length,
      linksSkipados: nauSplit.skipped,
    }),
  );

  await writeJsonFile(acpdLinksPath, {
    fonte: acpdListingUrl,
    fontePesquisa: acpdListingData.searchUrl,
    executadoEm: new Date().toISOString(),
    totalCards: acpdListingData.totalCards,
    totalLinksDetalhe: acpdListingData.detailLinks.length,
    totalLinksExternos: acpdListingData.links.length,
    totalDetalhesSemLinkExterno: acpdListingData.detailsWithoutExternalLink.length,
    detalhesSemLinkExterno: acpdListingData.detailsWithoutExternalLink,
    detalhes: acpdListingData.details,
    linksDetalhe: acpdListingData.detailLinks,
    linksExternos: acpdListingData.links,
  });

  await writeJsonFile(
    acpdCoursesRawJsonPath,
    buildCoursePayload({
      fonte: acpdListingUrl,
      dataExecucao: runDateIso,
      fusoHorario: TIME_ZONE,
      totalLinks: acpdListingData.links.length,
      totalDisponiveis: acpdSplitRaw.included.length,
      totalSkipados: acpdSplitRaw.skipped.length,
      cursos: acpdSplitRaw.included,
      extra: {
        totalLinksDetalhe: acpdListingData.detailLinks.length,
      },
    }),
  );

  const savedAcpdCoursesRawExcelPath = writeCoursesWorkbook(
    acpdSplitRaw.included,
    acpdCoursesRawExcelPath,
  );

  await writeJsonFile(
    acpdCoursesJsonPath,
    buildCoursePayload({
      fonte: acpdListingUrl,
      dataExecucao: runDateIso,
      fusoHorario: TIME_ZONE,
      totalLinks: acpdListingData.links.length,
      totalDisponiveis: acpdIncluded.length,
      totalSkipados: acpdSplitRaw.skipped.length,
      cursos: acpdIncluded,
      extra: {
        totalLinksDetalhe: acpdListingData.detailLinks.length,
        totalDisponiveisBruto: acpdSplitRaw.included.length,
        totalDuplicadosIgnorados: acpdDeduped.duplicatesAgainstPrimary.length,
        totalDuplicadosInternosIgnorados:
          acpdDeduped.duplicatesWithinSecondary.length,
      },
    }),
  );

  const savedAcpdCoursesExcelPath = writeCoursesWorkbook(
    acpdIncluded,
    acpdCoursesExcelPath,
  );

  await writeJsonFile(
    acpdSkippedPath,
    buildSkippedPayload({
      fonte: acpdListingUrl,
      dataExecucao: runDateIso,
      totalSkipados: acpdSplitRaw.skipped.length,
      linksSkipados: acpdSplitRaw.skipped,
      extra: {
        totalLinks: acpdListingData.links.length,
      },
    }),
  );

  await writeJsonFile(acpdDuplicatesPath, {
    fonte: acpdListingUrl,
    dataExecucao: runDateIso,
    totalDuplicadosIgnorados: acpdDeduped.duplicatesAgainstPrimary.length,
    totalDuplicadosInternosIgnorados:
      acpdDeduped.duplicatesWithinSecondary.length,
    duplicadosIgnorados: acpdDeduped.duplicatesAgainstPrimary,
    duplicadosInternosIgnorados: acpdDeduped.duplicatesWithinSecondary,
  });

  await writeJsonFile(
    combinedCoursesJsonPath,
    buildCoursePayload({
      fonte: {
        nau: nauListingUrl,
        acpd: acpdListingUrl,
      },
      dataExecucao: runDateIso,
      fusoHorario: TIME_ZONE,
      totalLinks: nauListingData.links.length + acpdListingData.links.length,
      totalDisponiveis: combinedIncluded.length,
      totalSkipados: combinedSkipped.length,
      cursos: combinedIncluded,
      extra: {
        origens: {
          nau: {
            totalLinks: nauListingData.links.length,
            totalDisponiveis: nauSplit.included.length,
            totalSkipados: nauSplit.skipped.length,
          },
          acpd: {
            totalLinks: acpdListingData.links.length,
            totalLinksDetalhe: acpdListingData.detailLinks.length,
            totalDisponiveisBruto: acpdSplitRaw.included.length,
            totalDisponiveis: acpdIncluded.length,
            totalSkipados: acpdSplitRaw.skipped.length,
            totalDuplicadosIgnorados:
              acpdDeduped.duplicatesAgainstPrimary.length,
            totalDuplicadosInternosIgnorados:
              acpdDeduped.duplicatesWithinSecondary.length,
          },
        },
      },
    }),
  );

  const savedCombinedCoursesExcelPath = writeCoursesWorkbook(
    combinedIncluded,
    combinedCoursesExcelPath,
  );
  const savedUniqueAreasExcelPath = writeAreasWorkbook(
    uniqueAreas,
    uniqueAreasExcelPath,
  );

  try {
    await unlink(legacyCoursesCsvPath);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }

  await writeJsonFile(
    combinedSkippedPath,
    buildSkippedPayload({
      fonte: {
        nau: nauListingUrl,
        acpd: acpdListingUrl,
      },
      dataExecucao: runDateIso,
      totalSkipados: combinedSkipped.length,
      linksSkipados: combinedSkipped,
      extra: {
        origens: {
          nau: {
            totalSkipados: nauSplit.skipped.length,
          },
          acpd: {
            totalSkipados: acpdSplitRaw.skipped.length,
          },
        },
      },
    }),
  );

  console.log(`Concluido. Cursos NAU base: ${nauSplit.included.length}`);
  console.log(
    `Concluido. Cursos ACPD brutos: ${acpdSplitRaw.included.length} | duplicados ignorados: ${acpdDeduped.duplicatesAgainstPrimary.length}`,
  );
  console.log(`Concluido. Cursos finais combinados: ${combinedIncluded.length}`);
  console.log(`NAU base JSON: ${nauCoursesBaseJsonPath}`);
  console.log(`NAU base Excel: ${savedNauCoursesBaseExcelPath}`);
  console.log(`ACPD raw JSON: ${acpdCoursesRawJsonPath}`);
  console.log(`ACPD raw Excel: ${savedAcpdCoursesRawExcelPath}`);
  console.log(`ACPD final JSON: ${acpdCoursesJsonPath}`);
  console.log(`ACPD final Excel: ${savedAcpdCoursesExcelPath}`);
  console.log(`JSON final combinado: ${combinedCoursesJsonPath}`);
  console.log(`Excel final combinado: ${savedCombinedCoursesExcelPath}`);
  console.log(`Areas: ${savedUniqueAreasExcelPath}`);
  console.log(`Skipados finais: ${combinedSkippedPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
