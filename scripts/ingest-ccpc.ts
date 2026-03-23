/**
 * Ingestion crawler for the CCPC (Competition and Consumer Protection
 * Commission Ireland) MCP server.
 *
 * Scrapes merger notifications, criminal enforcement cases, civil competition
 * enforcement cases, and market studies from ccpc.ie, then populates the
 * SQLite database.
 *
 * Data sources:
 *   - Merger Notifications    — /business/mergers/merger-notifications/
 *     82 pages of paginated listings (10 per page), each linking to a detail
 *     page with structured fields: case number, parties, notification date,
 *     decision date, economic sector, outcome, phase, PDF determination.
 *     Pagination: /page/N/
 *
 *   - Criminal Court Cases    — /business/enforcement/criminal-enforcement/criminal-court-cases/
 *     Single page listing cartel and other criminal competition cases with
 *     links to detail pages containing conviction details, fines, sentences.
 *
 *   - Civil Court Cases       — /business/enforcement/civil-competition-enforcement/civil-court-cases/
 *     Single page listing civil competition enforcement cases (resale price
 *     maintenance, abuse of dominance, collective boycott) with links to
 *     detail pages.
 *
 *   - Market Studies          — /business/research/market_studies/
 *     Paginated listing (2 pages) of sector inquiries and market reports.
 *
 * Usage:
 *   npx tsx scripts/ingest-ccpc.ts
 *   npx tsx scripts/ingest-ccpc.ts --dry-run
 *   npx tsx scripts/ingest-ccpc.ts --resume
 *   npx tsx scripts/ingest-ccpc.ts --force
 *   npx tsx scripts/ingest-ccpc.ts --max-pages 5
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["CCPC_DB_PATH"] ?? "data/ccpc.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.ccpc.ie";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarCCPCCrawler/1.0 (+https://github.com/Ansvar-Systems/irish-competition-mcp)";

/**
 * Listing categories on ccpc.ie.
 *
 * Merger notifications use /page/N/ pagination (82 pages, 10 per page).
 * Enforcement pages are single-page listings with links to detail pages.
 * Market studies use /page/N/ pagination (2 pages).
 */
const LISTING_CATEGORIES = [
  {
    id: "merger-notifications",
    path: "/business/mergers/merger-notifications/",
    detailPathPrefix: "/business/mergers-acquisitions/merger-notifications/",
    maxPages: 85,
    paginationStyle: "path" as const, // /page/N/
    isMerger: true,
  },
  {
    id: "criminal-court-cases",
    path: "/business/enforcement/criminal-enforcement/criminal-court-cases/",
    detailPathPrefix:
      "/business/enforcement/criminal-enforcement/criminal-court-cases/",
    maxPages: 1,
    paginationStyle: "none" as const,
    isMerger: false,
  },
  {
    id: "civil-court-cases",
    path: "/business/enforcement/civil-competition-enforcement/civil-court-cases/",
    detailPathPrefix:
      "/business/enforcement/civil-competition-enforcement/civil-court-cases/",
    maxPages: 1,
    paginationStyle: "none" as const,
    isMerger: false,
  },
  {
    id: "market-studies",
    path: "/business/research/market_studies/",
    detailPathPrefix: "/business/research/market-studies/",
    maxPages: 5,
    paginationStyle: "path" as const,
    isMerger: false,
  },
] as const;

type Category = (typeof LISTING_CATEGORIES)[number];

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPagesOverride = maxPagesArg ? parseInt(maxPagesArg, 10) : null;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-IE,en;q=0.9",
        },
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Listing page parsing — discover individual detail page URLs
// ---------------------------------------------------------------------------

/**
 * Crawl paginated listing pages to discover detail page URLs.
 *
 * CCPC merger notifications use /page/N/ pagination with ~10 entries per page.
 * Enforcement pages are single-page listings with links to case detail pages.
 * Market studies use /page/N/ pagination with ~10 entries per page.
 *
 * Each entry is an anchor tag whose href points to a detail page under the
 * category's detailPathPrefix (which may differ from the listing path).
 */
async function discoverUrlsFromListings(
  category: Category,
  maxPages: number,
): Promise<string[]> {
  const urls: string[] = [];
  const effectiveMax =
    maxPagesOverride && category.paginationStyle !== "none"
      ? Math.min(maxPagesOverride, maxPages)
      : maxPages;

  console.log(
    `\n  Discovering URLs from ${category.id} (up to ${effectiveMax} pages)...`,
  );

  for (let page = 1; page <= effectiveMax; page++) {
    let listUrl: string;
    if (page === 1 || category.paginationStyle === "none") {
      listUrl = `${BASE_URL}${category.path}`;
    } else {
      listUrl = `${BASE_URL}${category.path}page/${page}/`;
    }

    if (page % 10 === 1 || page === 1) {
      console.log(
        `    Fetching listing page ${page}/${effectiveMax}... (${urls.length} URLs so far)`,
      );
    }

    const html = await rateLimitedFetch(listUrl);
    if (!html) {
      console.warn(`    [WARN] Could not fetch listing page ${page}`);
      continue;
    }

    const $ = cheerio.load(html);
    let pageUrls = 0;

    $("a[href]").each((_i, el) => {
      const href = $(el).attr("href");
      if (!href) return;

      // Normalise: strip origin if present, keep path
      let path = href;
      if (href.startsWith(BASE_URL)) {
        path = href.slice(BASE_URL.length);
      }

      // Only accept links to detail pages under the detailPathPrefix
      // and reject the category index itself, pagination links, and anchors.
      if (
        path.startsWith(category.detailPathPrefix) &&
        path !== category.detailPathPrefix &&
        path !== category.path &&
        !path.includes("/page/") &&
        !path.includes("?") &&
        !path.includes("#") &&
        path.length > category.detailPathPrefix.length + 3
      ) {
        const fullUrl = path.startsWith("http")
          ? path
          : `${BASE_URL}${path}`;
        if (!urls.includes(fullUrl)) {
          urls.push(fullUrl);
          pageUrls++;
        }
      }
    });

    // If no new URLs found on this page, we have exhausted the listing
    if (pageUrls === 0 && page > 1) {
      console.log(
        `    No new URLs on page ${page} -- stopping pagination for ${category.id}`,
      );
      break;
    }

    // Single-page categories: stop after first page
    if (category.paginationStyle === "none") break;
  }

  console.log(`    Discovered ${urls.length} URLs from ${category.id}`);
  return urls;
}

// ---------------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------------

/**
 * Parse dates found on CCPC pages.
 *
 * CCPC uses several formats:
 *   - "Thursday, February 19, 2026"  (merger detail pages)
 *   - "19/02/2026" or "19/2/2026"    (European day/month/year)
 *   - "dd/MM/yyyy"                   (market studies listing)
 *   - "2026-02-19"                   (ISO — already good)
 *   - "15 April 2021"               (enforcement pages)
 *   - "31 May 2017"                 (conviction pages)
 */
const ENGLISH_MONTHS: Record<string, string> = {
  january: "01",
  february: "02",
  march: "03",
  april: "04",
  may: "05",
  june: "06",
  july: "07",
  august: "08",
  september: "09",
  october: "10",
  november: "11",
  december: "12",
};

function parseDate(raw: string): string | null {
  if (!raw) return null;
  const trimmed = raw.trim();

  // ISO: yyyy-MM-dd
  const isoMatch = trimmed.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) return isoMatch[0]!;

  // "Thursday, February 19, 2026" or "February 19, 2026"
  const usTextMatch = trimmed.match(
    /(?:\w+,\s+)?(\w+)\s+(\d{1,2}),?\s+(\d{4})/,
  );
  if (usTextMatch) {
    const monthNum = ENGLISH_MONTHS[usTextMatch[1]!.toLowerCase()];
    if (monthNum) {
      return `${usTextMatch[3]}-${monthNum}-${usTextMatch[2]!.padStart(2, "0")}`;
    }
  }

  // "15 April 2021" or "31 May 2017"
  const euTextMatch = trimmed.match(/(\d{1,2})\s+(\w+)\s+(\d{4})/);
  if (euTextMatch) {
    const monthNum = ENGLISH_MONTHS[euTextMatch[2]!.toLowerCase()];
    if (monthNum) {
      return `${euTextMatch[3]}-${monthNum}-${euTextMatch[1]!.padStart(2, "0")}`;
    }
  }

  // dd/MM/yyyy
  const slashMatch = trimmed.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (slashMatch) {
    return `${slashMatch[3]}-${slashMatch[2]!.padStart(2, "0")}-${slashMatch[1]!.padStart(2, "0")}`;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Merger detail page parsing
// ---------------------------------------------------------------------------

/**
 * Parse a CCPC merger notification detail page.
 *
 * CCPC merger detail pages display structured fields in a table-like layout:
 *   - Notification date, Decision date
 *   - Parties Involved (with business activities)
 *   - Economic sector
 *   - Phase (Phase 1 / Phase 2)
 *   - Current status (Active / Completed)
 *   - PDF determination link
 *
 * The title uses "Acquiring / Target" format (e.g. "CD&R/Top Security Group").
 */
function parseMergerPage(
  html: string,
  url: string,
): ParsedMerger | null {
  const $ = cheerio.load(html);

  // --- Title ---
  const rawTitle =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    "";
  if (!rawTitle) return null;

  // Clean title: remove trailing " - CCPC Business" etc.
  const title = rawTitle
    .replace(/\s*[-–|]\s*(?:CCPC|Competition).*$/i, "")
    .trim();

  // --- Case number from title or URL ---
  // Merger titles on CCPC start with the case number: "M/26/012" or similar.
  // Also found in the URL slug: m-26-012-cdr-top-security-group
  let caseNumber: string | null = null;

  // From page content: look for "M/YY/NNN" pattern
  const pageText = $("body").text();
  const caseMatch = pageText.match(/M\/\d{2}\/\d{3}/);
  if (caseMatch) {
    caseNumber = caseMatch[0];
  }

  // Fallback: extract from URL slug
  if (!caseNumber) {
    const slugMatch = url.match(/\/m-(\d{2})-(\d{3})/);
    if (slugMatch) {
      caseNumber = `M/${slugMatch[1]}/${slugMatch[2]}`;
    }
  }

  // Last resort: generate from URL
  if (!caseNumber) {
    const slug = url.split("/").filter(Boolean).pop() ?? "";
    caseNumber = `CCPC-WEB/${slug.slice(0, 80)}`;
  }

  // --- Dates ---
  // Look for labelled date fields in the page
  let notificationDate: string | null = null;
  let decisionDate: string | null = null;

  // Strategy: scan all text nodes for "Notification Date" / "Decision Date" labels
  $("td, dd, p, span, div").each((_i, el) => {
    const text = $(el).text().trim();

    if (/notification\s+date/i.test(text)) {
      // The value might be in the next sibling td/dd or within the same element
      const nextEl = $(el).next();
      const dateText = nextEl.length
        ? nextEl.text().trim()
        : text.replace(/.*notification\s+date[:\s]*/i, "").trim();
      notificationDate = parseDate(dateText);
    }

    if (/decision\s+date/i.test(text)) {
      const nextEl = $(el).next();
      const dateText = nextEl.length
        ? nextEl.text().trim()
        : text.replace(/.*decision\s+date[:\s]*/i, "").trim();
      decisionDate = parseDate(dateText);
    }
  });

  // Fallback: look for date patterns in first portion of body
  if (!notificationDate && !decisionDate) {
    const bodySlice = pageText.slice(0, 3000);
    // "Notification Date: Thursday, February 19, 2026"
    const notifMatch = bodySlice.match(
      /Notification\s+Date[:\s]+(.+?)(?:\n|Decision|Third)/i,
    );
    if (notifMatch) {
      notificationDate = parseDate(notifMatch[1]!);
    }
    const decMatch = bodySlice.match(
      /Decision\s+Date[:\s]+(.+?)(?:\n|Third|Phase|Current)/i,
    );
    if (decMatch) {
      decisionDate = parseDate(decMatch[1]!);
    }
  }

  const date = decisionDate ?? notificationDate;

  // --- Economic sector ---
  let sector: string | null = null;
  const sectorMatch = pageText.match(
    /Economic\s+[Ss]ector[:\s]+([^\n]+)/i,
  );
  if (sectorMatch) {
    sector = classifySectorFromLabel(sectorMatch[1]!.trim());
  }

  // --- Parties (acquiring / target) ---
  const { acquiring, target } = extractMergerParties(title, pageText);

  // --- Outcome / Phase ---
  const outcome = classifyMergerOutcome(title, pageText);

  // --- Body text ---
  const bodyText = extractBodyText($);
  if (!bodyText || bodyText.length < 30) return null;

  // --- Summary (first 500 chars of body) ---
  const summary = bodyText.slice(0, 500).replace(/\s+/g, " ").trim();

  return {
    case_number: caseNumber,
    title,
    date,
    sector,
    acquiring_party: acquiring,
    target,
    summary,
    full_text: bodyText,
    outcome,
    turnover: null, // Turnover not reliably available on CCPC pages
  };
}

// ---------------------------------------------------------------------------
// Enforcement / decision detail page parsing
// ---------------------------------------------------------------------------

/**
 * Parse a CCPC enforcement decision detail page.
 *
 * Criminal and civil enforcement pages present case details as narrative
 * text with embedded data: defendants, offences, penalties, dates, and
 * legal provisions. The structure is less uniform than merger pages.
 */
function parseDecisionPage(
  html: string,
  url: string,
  categoryId: string,
): ParsedDecision | null {
  const $ = cheerio.load(html);

  // --- Title ---
  const rawTitle =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    "";
  if (!rawTitle) return null;

  const title = rawTitle
    .replace(/\s*[-–|]\s*(?:CCPC|Competition).*$/i, "")
    .trim();

  // --- Case number ---
  // Enforcement pages rarely have explicit case numbers. Generate from URL slug.
  const slug = url.split("/").filter(Boolean).pop() ?? "";
  const caseNumber = `CCPC/${categoryId}/${slug.slice(0, 80)}`;

  // --- Body text ---
  const bodyText = extractBodyText($);
  if (!bodyText || bodyText.length < 30) return null;

  // --- Date ---
  let date: string | null = null;

  // Look for dates in the page text. CCPC enforcement pages often have
  // dates like "31 May 2017" or "15 April 2021" embedded in the text.
  const datePatterns = [
    // "convicted on 31 May 2017" / "sentenced on 13 June 2025"
    /(?:convicted|sentenced|pleaded|ordered|agreed|judgment|decided|delivered)\s+(?:on\s+)?(\d{1,2}\s+\w+\s+\d{4})/gi,
    // "Agreement Date: 15 April 2021"
    /(?:agreement|court\s+order|investigation)\s+(?:date)?[:\s]+(\d{1,2}\s+\w+\s+\d{4})/gi,
    // Standalone prominent date: "13 June 2025"
    /(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})/gi,
  ];

  for (const pattern of datePatterns) {
    const match = pattern.exec(bodyText);
    if (match?.[1]) {
      date = parseDate(match[1]);
      if (date) break;
    }
  }

  // --- Type classification ---
  const { type, outcome } = classifyDecisionType(categoryId, title, bodyText);

  // --- Parties ---
  const parties = extractParties(title, bodyText);

  // --- Fine amount ---
  const fineAmount = extractFineAmount(bodyText);

  // --- Legal articles ---
  const legalArticles = extractLegalArticles(bodyText);

  // --- Sector ---
  const sector = classifySector(title, bodyText);

  // --- Summary ---
  const summary = bodyText.slice(0, 500).replace(/\s+/g, " ").trim();

  // --- Status ---
  const status = bodyText.toLowerCase().includes("ongoing") ||
    bodyText.toLowerCase().includes("under investigation")
    ? "ongoing"
    : "final";

  return {
    case_number: caseNumber,
    title,
    date,
    type,
    sector,
    parties: parties ? JSON.stringify(parties) : null,
    summary,
    full_text: bodyText,
    outcome: outcome ?? (fineAmount ? "fine" : null),
    fine_amount: fineAmount,
    gwb_articles: legalArticles.length > 0 ? JSON.stringify(legalArticles) : null,
    status,
  };
}

// ---------------------------------------------------------------------------
// Shared extraction helpers
// ---------------------------------------------------------------------------

/**
 * Extract the main body text from a CCPC page, stripping navigation,
 * footer, and sidebar content.
 */
function extractBodyText($: cheerio.CheerioAPI): string {
  // CCPC uses a WordPress theme. The main content is typically inside
  // .entry-content, article, or .inner-content.
  const bodySelectors = [
    ".entry-content",
    "article .entry-content",
    ".inner-content",
    "article",
    ".content-area",
    "main",
  ];

  let bodyText = "";
  for (const sel of bodySelectors) {
    const el = $(sel);
    if (el.length > 0) {
      // Clone and strip unwanted elements
      const clone = el.clone();
      clone.find("nav, footer, header, .menu, .breadcrumb, script, style, .skip-link, .sidebar, .widget").remove();
      bodyText = clone.text().trim();
      if (bodyText.length > 100) break;
    }
  }

  // Fallback: gather all paragraphs from main/article
  if (!bodyText || bodyText.length < 100) {
    const paragraphs: string[] = [];
    $("main p, article p, .content p").each((_i, el) => {
      const text = $(el).text().trim();
      if (text.length > 20) paragraphs.push(text);
    });
    bodyText = paragraphs.join("\n\n");
  }

  // Last resort: strip navigation and take everything
  if (!bodyText || bodyText.length < 50) {
    $(
      "nav, footer, header, .menu, .breadcrumb, script, style, .skip-link",
    ).remove();
    bodyText = $("main, article, .content, body").text().trim();
  }

  // Clean up excessive whitespace
  return bodyText.replace(/\n{3,}/g, "\n\n").replace(/[ \t]+/g, " ").trim();
}

/**
 * Extract parties from enforcement case title and body text.
 *
 * CCPC enforcement titles often contain the party names:
 *   "Commercial Flooring Cartel Conviction"
 *   "CCPC secures commitments in Resale Price Maintenance case"
 *   "Members of Home Heating Oil Cartel convicted of price fixing"
 */
function extractParties(
  title: string,
  bodyText: string,
): string[] | null {
  const parties: string[] = [];

  // Look for named defendants in the body text
  // "Mr Brendan Smith" / "Mr Patrick Doyle" / "Mr Ali Fawad"
  const personPattern = /(?:Mr|Ms|Mrs)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})/g;
  let match: RegExpExecArray | null;
  while ((match = personPattern.exec(bodyText)) !== null) {
    const name = match[0]!.trim();
    if (!parties.includes(name)) {
      parties.push(name);
    }
  }

  // Look for company names near defendant/respondent/accused keywords
  const companyPattern =
    /(?:respondent|defendant|accused|against|investigation\s+(?:of|into))\s+([A-Z][A-Za-z\s&']+(?:Ltd|Limited|plc|Inc|Group|Association|Organisation))/gi;
  while ((match = companyPattern.exec(bodyText)) !== null) {
    const name = match[1]!.trim();
    if (name.length > 3 && name.length < 100 && !parties.includes(name)) {
      parties.push(name);
    }
  }

  // Extract from title if it mentions specific entities
  // e.g. "LVA and VFI in contempt of court"
  const titleOrgPattern =
    /\b([A-Z][A-Z]{1,6})\b/g;
  while ((match = titleOrgPattern.exec(title)) !== null) {
    const acronym = match[1]!;
    // Skip common false positives
    if (
      !["CCPC", "THE", "AND", "FOR", "DPP", "HIGH", "RPM"].includes(acronym) &&
      acronym.length >= 2 &&
      !parties.includes(acronym)
    ) {
      parties.push(acronym);
    }
  }

  return parties.length > 0 ? parties : null;
}

/**
 * Extract acquiring party and target from a merger title.
 *
 * CCPC merger titles use formats:
 *   "CD&R/Top Security Group"
 *   "Samsung Biologics/Human Genome"
 *   "United Hardware/Expert Hardware"
 */
function extractMergerParties(
  title: string,
  bodyText: string,
): { acquiring: string | null; target: string | null } {
  // Strip the case number prefix (e.g. "M/26/012 - " or "M/26/012- ")
  const cleanTitle = title.replace(/^M\/\d{2}\/\d{3}\s*[-–]\s*/, "").trim();

  // Primary pattern: "X / Y" or "X/Y" in the title
  const slashParts = cleanTitle.split(/\s*\/\s*/);
  if (slashParts.length >= 2) {
    return {
      acquiring: slashParts[0]!.trim().slice(0, 300),
      target: slashParts.slice(1).join(" / ").trim().slice(0, 300),
    };
  }

  // Fallback: look for "acquisition by X of Y" patterns in body
  const bodyMatch = bodyText.match(
    /acquisition\s+by\s+(.{3,80}?)\s+of\s+(.{3,80}?)(?:\.|,)/i,
  );
  if (bodyMatch) {
    return {
      acquiring: bodyMatch[1]!.trim(),
      target: bodyMatch[2]!.trim(),
    };
  }

  return { acquiring: cleanTitle || null, target: null };
}

/**
 * Extract a fine/penalty amount from text.
 *
 * CCPC fines are in euros. Handles patterns like:
 *   "fined €45,000"
 *   "fined €10,000"
 *   "€4,000 fine"
 *   "ordered to pay €8,241"
 *   "compensation of €8,277"
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    // "fined €45,000" / "fine of €10,000"
    /fin(?:ed?|es?)\s+(?:of\s+)?€([\d,.\s]+)/gi,
    // "€4,000 fine" / "€10,000,000 penalty"
    /€([\d,.\s]+)\s+(?:fine|penalty|sanction)/gi,
    // "ordered to pay €8,241"
    /(?:ordered|required)\s+to\s+pay\s+€([\d,.\s]+)/gi,
    // Generic "€N" near penalty keywords
    /(?:penalty|sanction|fine|compensation)[^€]{0,30}€([\d,.\s]+)/gi,
    // "€N" followed by context
    /€([\d,.\s]+)/gi,
  ];

  let bestAmount: number | null = null;

  for (const pattern of patterns) {
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      if (match[1]) {
        const numStr = match[1].trim().replace(/[\s,]/g, "");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) {
          // Take the largest fine mentioned (often multiple amounts appear)
          if (bestAmount === null || val > bestAmount) {
            bestAmount = val;
          }
        }
      }
    }
  }

  // Only return amounts that look like actual fines (> €100)
  return bestAmount !== null && bestAmount >= 100 ? bestAmount : null;
}

/**
 * Extract cited Irish competition law provisions and EU treaty articles.
 */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();
  let m: RegExpExecArray | null;

  // Section N of the Competition Act 2002
  const compActPattern =
    /[Ss]ection\s+(\d+)\s+(?:of\s+)?(?:the\s+)?Competition\s+Act\s+2002/gi;
  while ((m = compActPattern.exec(text)) !== null) {
    articles.add(`Section ${m[1]} Competition Act 2002`);
  }

  // Section N of the Competition and Consumer Protection Act 2014
  const ccpaPattern =
    /[Ss]ection\s+(\d+)\s+(?:of\s+)?(?:the\s+)?(?:Competition\s+and\s+Consumer\s+Protection\s+Act|CCPA)\s+2014/gi;
  while ((m = ccpaPattern.exec(text)) !== null) {
    articles.add(`Section ${m[1]} CCPA 2014`);
  }

  // Article 101 / 102 TFEU
  const tfeuPattern =
    /[Aa]rticle\s+(101|102)\s+(?:of\s+)?(?:the\s+)?(?:TFEU|Treaty\s+on\s+the\s+Functioning)/gi;
  while ((m = tfeuPattern.exec(text)) !== null) {
    articles.add(`Article ${m[1]} TFEU`);
  }

  // Standalone "Article 101" / "Article 102" mentions
  const artPattern = /\b[Aa]rt(?:icle)?\.?\s*(101|102)\b/gi;
  while ((m = artPattern.exec(text)) !== null) {
    articles.add(`Article ${m[1]} TFEU`);
  }

  // Consumer Protection Act 2007
  const consumerPattern =
    /Consumer\s+Protection\s+Act\s+2007/gi;
  if (consumerPattern.test(text)) {
    articles.add("Consumer Protection Act 2007");
  }

  // European Accessibility Act
  const accessPattern = /European\s+Accessibility\s+Act/gi;
  if (accessPattern.test(text)) {
    articles.add("European Accessibility Act");
  }

  // Digital Markets Act
  if (/Digital\s+Markets\s+Act/i.test(text)) {
    articles.add("Digital Markets Act");
  }

  return [...articles];
}

/**
 * Classify a decision type based on the category, title, and body.
 */
function classifyDecisionType(
  categoryId: string,
  title: string,
  bodyText: string,
): { type: string | null; outcome: string | null } {
  const lowerTitle = title.toLowerCase();
  const lowerBody = bodyText.toLowerCase().slice(0, 3000);
  const all = `${lowerTitle} ${lowerBody}`;

  // --- Type classification ---
  let type: string | null = null;

  if (categoryId === "criminal-court-cases") {
    // Criminal enforcement cases
    if (
      all.includes("cartel") ||
      all.includes("price fixing") ||
      all.includes("bid-rigging") ||
      all.includes("bid rigging")
    ) {
      type = "cartel";
    } else if (all.includes("misleading") || all.includes("false information")) {
      type = "consumer_protection";
    } else {
      type = "criminal_enforcement";
    }
  } else if (categoryId === "civil-court-cases") {
    if (all.includes("resale price maintenance") || all.includes("rpm")) {
      type = "resale_price_maintenance";
    } else if (
      all.includes("abuse of dominan") ||
      all.includes("dominant position")
    ) {
      type = "abuse_of_dominance";
    } else if (all.includes("collective boycott")) {
      type = "collective_boycott";
    } else if (all.includes("price fixing") || all.includes("price-fixing")) {
      type = "cartel";
    } else if (
      all.includes("reduce capacity") ||
      all.includes("capacity reduction")
    ) {
      type = "capacity_restriction";
    } else if (all.includes("contempt of court")) {
      type = "contempt";
    } else {
      type = "civil_enforcement";
    }
  } else if (categoryId === "market-studies") {
    type = "sector_inquiry";
  } else {
    type = "decision";
  }

  // --- Outcome classification ---
  let outcome: string | null = null;

  if (
    all.includes("convicted") ||
    all.includes("conviction") ||
    all.includes("pleaded guilty") ||
    all.includes("pleads guilty")
  ) {
    outcome = "convicted";
  } else if (
    all.includes("fined") ||
    all.includes("fine of") ||
    all.includes("penalty of")
  ) {
    outcome = "fine";
  } else if (
    all.includes("commitments") ||
    all.includes("undertaking") ||
    all.includes("conditions")
  ) {
    outcome = "cleared_with_conditions";
  } else if (
    all.includes("acquitted") ||
    all.includes("dismissed") ||
    all.includes("struck out")
  ) {
    outcome = "dismissed";
  } else if (
    all.includes("settlement") ||
    all.includes("settled")
  ) {
    outcome = "settlement";
  } else if (all.includes("contempt")) {
    outcome = "contempt";
  } else if (categoryId === "market-studies") {
    outcome = "published";
  }

  return { type, outcome };
}

/**
 * Classify a merger outcome based on page content.
 */
function classifyMergerOutcome(
  title: string,
  bodyText: string,
): string | null {
  const all = `${title} ${bodyText}`.toLowerCase();

  if (all.includes("blocked") || all.includes("prohibited")) {
    return "blocked";
  }
  if (
    all.includes("cleared with conditions") ||
    all.includes("subject to conditions") ||
    all.includes("conditional clearance") ||
    all.includes("required to divest")
  ) {
    return "cleared_with_conditions";
  }
  if (all.includes("withdrawn") || all.includes("notification withdrawn")) {
    return "withdrawn";
  }
  if (all.includes("phase 2") || all.includes("phase ii")) {
    if (all.includes("cleared") || all.includes("approved")) {
      return "cleared_phase2";
    }
    return "phase2_review";
  }
  if (
    all.includes("phase 1") ||
    all.includes("phase i ") ||
    all.includes("cleared") ||
    all.includes("approved") ||
    all.includes("completed")
  ) {
    return "cleared_phase1";
  }
  if (all.includes("active")) {
    return "pending";
  }

  return "cleared_phase1";
}

/**
 * Map CCPC economic sector labels to sector IDs.
 *
 * CCPC merger pages include an "Economic sector" field with values like
 * "Manufacturing", "Other Services", "Financial Services", etc.
 */
function classifySectorFromLabel(label: string): string | null {
  const lower = label.toLowerCase().trim();

  const labelMap: Record<string, string> = {
    "manufacturing": "manufacturing",
    "financial services": "financial_services",
    "financial intermediation": "financial_services",
    "banking": "financial_services",
    "insurance": "financial_services",
    "other services": "services",
    "wholesale and retail trade": "retail",
    "retail": "retail",
    "wholesale": "retail",
    "transport": "transport",
    "transport and storage": "transport",
    "transport, storage and communication": "transport",
    "communications": "telecommunications",
    "telecommunications": "telecommunications",
    "information and communication": "telecommunications",
    "construction": "construction",
    "energy": "energy",
    "electricity, gas and water supply": "energy",
    "health": "healthcare",
    "health and social work": "healthcare",
    "healthcare": "healthcare",
    "education": "education",
    "agriculture": "agriculture",
    "agriculture, hunting and forestry": "agriculture",
    "mining": "mining",
    "mining and quarrying": "mining",
    "real estate": "real_estate",
    "real estate, renting and business activities": "real_estate",
    "hotels and restaurants": "hospitality",
    "accommodation and food service activities": "hospitality",
    "media": "media",
    "technology": "digital_economy",
    "food and beverages": "food_and_beverages",
    "pharmaceutical": "healthcare",
    "automotive": "automotive",
  };

  return labelMap[lower] ?? null;
}

/**
 * Classify sector from title and body text using keyword matching.
 */
function classifySector(
  title: string,
  bodyText: string,
): string | null {
  const text = `${title} ${bodyText.slice(0, 2000)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "grocery",
      patterns: [
        "grocery",
        "supermarket",
        "dunnes",
        "tesco",
        "lidl",
        "aldi",
        "supervalu",
        "musgrave",
        "centra",
        "convenience store",
        "food retail",
      ],
    },
    {
      id: "financial_services",
      patterns: [
        "insurance",
        "banking",
        "bank",
        "financial",
        "payment",
        "credit",
        "mortgage",
        "fund",
        "investment",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telecom",
        "broadband",
        "mobile",
        "eir",
        "vodafone",
        "three ireland",
        "virgin media",
        "fibre",
        "5g",
      ],
    },
    {
      id: "healthcare",
      patterns: [
        "health",
        "hospital",
        "pharma",
        "medical",
        "gp ",
        "general practitioner",
        "dentist",
        "pharmacy",
        "drug",
      ],
    },
    {
      id: "digital_economy",
      patterns: [
        "digital",
        "online platform",
        "software",
        "technology",
        "app ",
        "e-commerce",
        "advertising",
        "internet",
      ],
    },
    {
      id: "energy",
      patterns: [
        "energy",
        "electricity",
        "gas",
        "oil",
        "fuel",
        "heating oil",
        "renewable",
        "wind",
        "solar",
        "esb",
      ],
    },
    {
      id: "media",
      patterns: [
        "media",
        "broadcast",
        "newspaper",
        "television",
        "radio",
        "ticketing",
        "ticketmaster",
        "entertainment",
      ],
    },
    {
      id: "construction",
      patterns: [
        "construction",
        "cement",
        "concrete",
        "building",
        "flooring",
        "property",
        "housing",
        "kingspan",
      ],
    },
    {
      id: "transport",
      patterns: [
        "transport",
        "logistics",
        "shipping",
        "aviation",
        "airline",
        "bus",
        "rail",
        "port",
        "freight",
      ],
    },
    {
      id: "agriculture",
      patterns: [
        "agriculture",
        "farming",
        "beef",
        "dairy",
        "livestock",
        "milk",
        "cattle",
      ],
    },
    {
      id: "automotive",
      patterns: [
        "car dealer",
        "motor",
        "vehicle",
        "ford dealer",
        "citroën",
        "citroen",
        "second-hand car",
        "used car",
      ],
    },
    {
      id: "hospitality",
      patterns: [
        "hotel",
        "hospitality",
        "pub",
        "alcohol",
        "vintner",
        "licensed premises",
        "restaurant",
      ],
    },
    {
      id: "waste_management",
      patterns: [
        "waste",
        "recycling",
        "waste collection",
        "household waste",
      ],
    },
    {
      id: "professional_services",
      patterns: [
        "legal",
        "accounting",
        "veterinary",
        "professional service",
        "consulting",
      ],
    },
    {
      id: "retail",
      patterns: [
        "retail",
        "footwear",
        "furniture",
        "consumer goods",
        "diy",
        "hardware",
      ],
    },
    {
      id: "manufacturing",
      patterns: [
        "manufacturing",
        "factory",
        "industrial",
        "processing",
        "biopharmaceutical",
      ],
    },
  ];

  for (const { id, patterns } of sectorMapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Sector metadata
// ---------------------------------------------------------------------------

const SECTOR_META: Record<string, { name: string; name_en: string }> = {
  grocery: { name: "Grocery Retail", name_en: "Grocery Retail" },
  financial_services: {
    name: "Financial Services",
    name_en: "Financial Services",
  },
  telecommunications: {
    name: "Telecommunications",
    name_en: "Telecommunications",
  },
  healthcare: { name: "Healthcare", name_en: "Healthcare" },
  digital_economy: { name: "Digital Economy", name_en: "Digital Economy" },
  energy: { name: "Energy", name_en: "Energy" },
  media: { name: "Media", name_en: "Media" },
  construction: { name: "Construction", name_en: "Construction" },
  transport: { name: "Transport", name_en: "Transport" },
  agriculture: { name: "Agriculture", name_en: "Agriculture" },
  automotive: { name: "Automotive", name_en: "Automotive" },
  hospitality: { name: "Hospitality", name_en: "Hospitality" },
  waste_management: {
    name: "Waste Management",
    name_en: "Waste Management",
  },
  professional_services: {
    name: "Professional Services",
    name_en: "Professional Services",
  },
  retail: { name: "Retail", name_en: "Retail" },
  manufacturing: { name: "Manufacturing", name_en: "Manufacturing" },
  services: { name: "Services", name_en: "Services" },
  real_estate: { name: "Real Estate", name_en: "Real Estate" },
  education: { name: "Education", name_en: "Education" },
  mining: { name: "Mining", name_en: "Mining" },
  food_and_beverages: {
    name: "Food and Beverages",
    name_en: "Food and Beverages",
  },
};

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== CCPC Competition Decisions Crawler ===");
  console.log(`  Database:    ${DB_PATH}`);
  console.log(`  Dry run:     ${dryRun}`);
  console.log(`  Resume:      ${resume}`);
  console.log(`  Force:       ${force}`);
  console.log(
    `  Max pages:   ${maxPagesOverride ?? "per-category defaults"}`,
  );
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // Step 1: Discover URLs from all listing categories
  const allUrls: Array<{ url: string; category: Category }> = [];

  for (const category of LISTING_CATEGORIES) {
    const urls = await discoverUrlsFromListings(category, category.maxPages);
    for (const url of urls) {
      allUrls.push({ url, category });
    }
  }

  // Deduplicate by URL
  const seenUrls = new Set<string>();
  const dedupedUrls = allUrls.filter(({ url }) => {
    if (seenUrls.has(url)) return false;
    seenUrls.add(url);
    return true;
  });

  // Filter already-processed URLs (for --resume)
  const urlsToProcess = resume
    ? dedupedUrls.filter(({ url }) => !processedSet.has(url))
    : dedupedUrls;

  console.log(`\nTotal discovered URLs: ${dedupedUrls.length}`);
  console.log(`URLs to process:       ${urlsToProcess.length}`);
  if (resume && dedupedUrls.length !== urlsToProcess.length) {
    console.log(
      `  Skipping ${dedupedUrls.length - urlsToProcess.length} already-processed URLs`,
    );
  }

  if (urlsToProcess.length === 0) {
    console.log("Nothing to process. Exiting.");
    return;
  }

  // Step 2: Initialize database (unless dry run)
  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // Step 3: Process each URL
  let decisionsIngested = 0;
  let mergersIngested = 0;
  let errors = 0;
  let skipped = 0;

  for (let i = 0; i < urlsToProcess.length; i++) {
    const { url, category } = urlsToProcess[i]!;
    const progress = `[${i + 1}/${urlsToProcess.length}]`;

    console.log(`${progress} ${category.id} | ${url}`);

    const html = await rateLimitedFetch(url);
    if (!html) {
      console.log(`  SKIP -- could not fetch`);
      state.errors.push(`fetch_failed: ${url}`);
      errors++;
      continue;
    }

    try {
      if (category.isMerger) {
        // --- Merger detail page ---
        const merger = parseMergerPage(html, url);

        if (merger) {
          if (dryRun) {
            console.log(
              `  MERGER: ${merger.case_number} -- ${merger.title.slice(0, 80)}`,
            );
            console.log(
              `    sector=${merger.sector}, outcome=${merger.outcome}, acquiring=${merger.acquiring_party?.slice(0, 50)}`,
            );
          } else {
            const stmt = force
              ? stmts!.upsertMerger
              : stmts!.insertMerger;
            stmt.run(
              merger.case_number,
              merger.title,
              merger.date,
              merger.sector,
              merger.acquiring_party,
              merger.target,
              merger.summary,
              merger.full_text,
              merger.outcome,
              merger.turnover,
            );
            console.log(`  INSERTED merger: ${merger.case_number}`);
          }
          mergersIngested++;
        } else {
          console.log(`  SKIP -- could not parse merger data`);
          skipped++;
        }
      } else {
        // --- Decision / enforcement detail page ---
        const decision = parseDecisionPage(html, url, category.id);

        if (decision) {
          if (dryRun) {
            console.log(
              `  DECISION: ${decision.case_number} -- ${decision.title.slice(0, 80)}`,
            );
            console.log(
              `    type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}, fine=${decision.fine_amount}`,
            );
          } else {
            const stmt = force
              ? stmts!.upsertDecision
              : stmts!.insertDecision;
            stmt.run(
              decision.case_number,
              decision.title,
              decision.date,
              decision.type,
              decision.sector,
              decision.parties,
              decision.summary,
              decision.full_text,
              decision.outcome,
              decision.fine_amount,
              decision.gwb_articles,
              decision.status,
            );
            console.log(`  INSERTED decision: ${decision.case_number}`);
          }
          decisionsIngested++;
        } else {
          console.log(`  SKIP -- could not parse decision data`);
          skipped++;
        }
      }

      // Mark URL as processed
      processedSet.add(url);
      state.processedUrls.push(url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`parse_error: ${url}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 URLs)
    if ((i + 1) % 25 === 0) {
      state.decisionsIngested += decisionsIngested;
      state.mergersIngested += mergersIngested;
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} URLs`);
    }
  }

  // Step 4: Update sector counts from the database
  if (!dryRun && db && stmts) {
    const decisionSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<
      string,
      { decisions: number; mergers: number }
    > = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(finalSectorCounts)) {
        const meta = SECTOR_META[id];
        stmts!.upsertSector.run(
          id,
          meta?.name ?? id,
          meta?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(
      `\nUpdated ${Object.keys(finalSectorCounts).length} sector records`,
    );
  }

  // Step 5: Final state save
  state.decisionsIngested += decisionsIngested;
  state.mergersIngested += mergersIngested;
  saveState(state);

  // Step 6: Summary
  if (!dryRun && db) {
    const decisionCount = (
      db.prepare("SELECT count(*) as cnt FROM decisions").get() as {
        cnt: number;
      }
    ).cnt;
    const mergerCount = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sectorCount = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\n=== Ingestion Complete ===");
    console.log(`  Decisions in DB:  ${decisionCount}`);
    console.log(`  Mergers in DB:    ${mergerCount}`);
    console.log(`  Sectors in DB:    ${sectorCount}`);
    console.log(`  New decisions:    ${decisionsIngested}`);
    console.log(`  New mergers:      ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
    console.log(`  State saved to:   ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry Run Complete ===");
    console.log(`  Decisions found:  ${decisionsIngested}`);
    console.log(`  Mergers found:    ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
  }

  console.log(`\nDone.`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
