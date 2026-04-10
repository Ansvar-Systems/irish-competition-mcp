#!/usr/bin/env node

/**
 * CCPC Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying CCPC decisions, merger control
 * cases, and sector enforcement activity under Competition Act 2002.
 *
 * Tool prefix: ie_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  searchDecisions,
  getDecision,
  searchMergers,
  getMerger,
  listSectors,
} from "./db.js";
import { buildCitation } from "./utils/citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "irish-competition-mcp";
const DATA_SOURCE_URL = "https://www.ccpc.ie/";

// --- Tool definitions ---------------------------------------------------------

const TOOLS = [
  {
    name: "ie_comp_search_decisions",
    description:
      "Full-text search across CCPC enforcement decisions (abuse of dominance, cartel, sector inquiries). Returns matching decisions with case number, parties, outcome, fine amount, and Competition Act articles cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'cartel', 'grocery retail', 'price fixing')",
        },
        type: {
          type: "string",
          enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"],
          description: "Filter by decision type. Optional.",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'digital_economy', 'grocery', 'financial_services'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"],
          description: "Filter by outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "ie_comp_get_decision",
    description:
      "Get a specific CCPC enforcement decision by case number (e.g., 'CCPC/E/2019/001', 'CCPC/D/2020/002').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "CCPC case number (e.g., 'CCPC/E/2019/001', 'CCPC/D/2020/002')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "ie_comp_search_mergers",
    description:
      "Search CCPC merger control decisions. Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'food retail merger', 'insurance acquisition', 'telecommunications')",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'energy', 'food_retail', 'financial_services'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"],
          description: "Filter by merger outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "ie_comp_get_merger",
    description:
      "Get a specific CCPC merger control decision by case number (e.g., 'M/18/001', 'M/20/015').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "CCPC merger case number (e.g., 'M/18/001', 'M/20/015')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "ie_comp_list_sectors",
    description:
      "List all sectors with CCPC enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "ie_comp_about",
    description:
      "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "ie_comp_list_sources",
    description:
      "Return the data sources used by this MCP server, including URLs, update frequency, and coverage.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "ie_comp_check_data_freshness",
    description:
      "Check when the database was last updated and whether it may be stale.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation --------------------------------------

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetDecisionArgs = z.object({
  case_number: z.string().min(1),
});

const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetMergerArgs = z.object({
  case_number: z.string().min(1),
});

// --- Helpers ------------------------------------------------------------------

function responseMeta() {
  return {
    disclaimer:
      "Research tool only — not legal or regulatory advice. Verify all references against primary sources before making compliance decisions.",
    data_age: "Periodic updates; may lag official CCPC publications.",
    copyright:
      "© Competition and Consumer Protection Commission (CCPC). Used for research purposes.",
    source_url: DATA_SOURCE_URL,
  };
}

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string, errorType = "tool_error") {
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(
          { error: message, _error_type: errorType, _meta: responseMeta() },
          null,
          2,
        ),
      },
    ],
    isError: true as const,
  };
}

// --- Server setup ------------------------------------------------------------

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "ie_comp_search_decisions": {
        const parsed = SearchDecisionsArgs.parse(args);
        const results = searchDecisions({
          query: parsed.query,
          type: parsed.type,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        const resultsWithCitations = results.map((r) => ({
          ...r,
          _citation: buildCitation(
            r.case_number,
            r.title,
            "ie_comp_get_decision",
            { case_number: r.case_number },
            DATA_SOURCE_URL,
          ),
        }));
        return textContent({
          results: resultsWithCitations,
          count: results.length,
          _meta: responseMeta(),
        });
      }

      case "ie_comp_get_decision": {
        const parsed = GetDecisionArgs.parse(args);
        const decision = getDecision(parsed.case_number);
        if (!decision) {
          return errorContent(`Decision not found: ${parsed.case_number}`, "not_found");
        }
        const dec = decision as unknown as Record<string, unknown>;
        return textContent({
          ...decision,
          _citation: buildCitation(
            String(dec.case_number ?? parsed.case_number),
            String(dec.title ?? dec.case_number ?? parsed.case_number),
            "ie_comp_get_decision",
            { case_number: parsed.case_number },
            DATA_SOURCE_URL,
          ),
          _meta: responseMeta(),
        });
      }

      case "ie_comp_search_mergers": {
        const parsed = SearchMergersArgs.parse(args);
        const results = searchMergers({
          query: parsed.query,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        const resultsWithCitations = results.map((r) => ({
          ...r,
          _citation: buildCitation(
            r.case_number,
            r.title,
            "ie_comp_get_merger",
            { case_number: r.case_number },
            DATA_SOURCE_URL,
          ),
        }));
        return textContent({
          results: resultsWithCitations,
          count: results.length,
          _meta: responseMeta(),
        });
      }

      case "ie_comp_get_merger": {
        const parsed = GetMergerArgs.parse(args);
        const merger = getMerger(parsed.case_number);
        if (!merger) {
          return errorContent(`Merger case not found: ${parsed.case_number}`, "not_found");
        }
        const mrg = merger as unknown as Record<string, unknown>;
        return textContent({
          ...merger,
          _citation: buildCitation(
            String(mrg.case_number ?? parsed.case_number),
            String(mrg.title ?? mrg.case_number ?? parsed.case_number),
            "ie_comp_get_merger",
            { case_number: parsed.case_number },
            DATA_SOURCE_URL,
          ),
          _meta: responseMeta(),
        });
      }

      case "ie_comp_list_sectors": {
        const sectors = listSectors();
        return textContent({ sectors, count: sectors.length, _meta: responseMeta() });
      }

      case "ie_comp_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "CCPC (Competition and Consumer Protection Commission) MCP server. Provides access to Irish competition law enforcement decisions, merger control cases, and sector enforcement data under the Competition Act 2002.",
          data_source: `CCPC (${DATA_SOURCE_URL})`,
          coverage: {
            decisions: "Abuse of dominance, cartel enforcement, and sector inquiries",
            mergers: "Merger control decisions — Phase I and Phase II",
            sectors: "Digital economy, energy, food retail, financial services, healthcare, media, telecommunications",
          },
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
          _meta: responseMeta(),
        });
      }

      case "ie_comp_list_sources": {
        return textContent({
          sources: [
            {
              name: "CCPC — Competition and Consumer Protection Commission",
              url: DATA_SOURCE_URL,
              types: ["enforcement_decisions", "merger_notifications"],
              update_frequency: "periodic",
              license: "Public sector information — see https://www.ccpc.ie/",
            },
          ],
          _meta: responseMeta(),
        });
      }

      case "ie_comp_check_data_freshness": {
        // Read ingest state if available
        let lastIngest: string | null = null;
        try {
          const raw = readFileSync(
            join(__dirname, "..", "data", "ingest-state.json"),
            "utf8",
          );
          const state = JSON.parse(raw) as { last_run?: string; last_updated?: string };
          lastIngest = state.last_run ?? state.last_updated ?? null;
        } catch {
          // state file absent — not an error
        }
        return textContent({
          last_ingest: lastIngest,
          status: lastIngest ? "available" : "unknown",
          note: "Database updates are periodic. Verify against https://www.ccpc.ie/ for the latest decisions.",
          _meta: responseMeta(),
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`, "unknown_tool");
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

// --- Main --------------------------------------------------------------------

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
