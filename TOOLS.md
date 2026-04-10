# Tool Reference

All tools use the `ie_comp_` prefix. Each response includes a `_meta` block with disclaimer, data age, copyright, and source URL.

---

## ie_comp_search_decisions

Full-text search across CCPC enforcement decisions (abuse of dominance, cartel, sector inquiries).

**Arguments**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | yes | Search query (e.g., `'cartel'`, `'grocery retail'`, `'price fixing'`) |
| `type` | string | no | Filter by type: `abuse_of_dominance`, `cartel`, `merger`, `sector_inquiry` |
| `sector` | string | no | Filter by sector ID (e.g., `'digital_economy'`, `'grocery'`) |
| `outcome` | string | no | Filter by outcome: `prohibited`, `cleared`, `cleared_with_conditions`, `fine` |
| `limit` | number | no | Maximum results to return (default: 20, max: 100) |

**Response**: `{ results: Decision[], count: number, _meta: Meta }`
Each result includes a per-item `_citation` block for entity linking.

---

## ie_comp_get_decision

Get a specific CCPC enforcement decision by case number.

**Arguments**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `case_number` | string | yes | CCPC case number (e.g., `'CCPC/E/2019/001'`) |

**Response**: Full `Decision` object with `_citation` and `_meta`.

---

## ie_comp_search_mergers

Search CCPC merger control decisions.

**Arguments**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | yes | Search query (e.g., `'food retail merger'`, `'insurance'`) |
| `sector` | string | no | Filter by sector ID |
| `outcome` | string | no | Filter: `cleared`, `cleared_phase1`, `cleared_with_conditions`, `prohibited` |
| `limit` | number | no | Maximum results (default: 20, max: 100) |

**Response**: `{ results: Merger[], count: number, _meta: Meta }`
Each result includes a per-item `_citation` block for entity linking.

---

## ie_comp_get_merger

Get a specific CCPC merger control decision by case number.

**Arguments**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `case_number` | string | yes | CCPC merger case number (e.g., `'M/18/001'`) |

**Response**: Full `Merger` object with `_citation` and `_meta`.

---

## ie_comp_list_sectors

List all sectors with CCPC enforcement activity.

**Arguments**: none

**Response**: `{ sectors: Sector[], count: number, _meta: Meta }`

---

## ie_comp_about

Return metadata about this MCP server.

**Arguments**: none

**Response**: Server name, version, description, data source, coverage summary, tool list, `_meta`.

---

## ie_comp_list_sources

Return the data sources used by this server.

**Arguments**: none

**Response**: `{ sources: Source[], _meta: Meta }` — includes source name, URL, data types, update frequency, and license.

---

## ie_comp_check_data_freshness

Check when the database was last updated.

**Arguments**: none

**Response**: `{ last_ingest: string|null, status: string, note: string, _meta: Meta }`

---

## Common Response Fields

### `_meta`

Present on every tool response.

```json
{
  "disclaimer": "Research tool only — not legal or regulatory advice. Verify all references against primary sources before making compliance decisions.",
  "data_age": "Periodic updates; may lag official CCPC publications.",
  "copyright": "© Competition and Consumer Protection Commission (CCPC). Used for research purposes.",
  "source_url": "https://www.ccpc.ie/"
}
```

### `_citation`

Present on `get_*` responses and per-item in `search_*` results.

```json
{
  "canonical_ref": "CCPC/E/2019/001",
  "display_text": "CCPC/E/2019/001",
  "source_url": "https://www.ccpc.ie/",
  "lookup": {
    "tool": "ie_comp_get_decision",
    "args": { "case_number": "CCPC/E/2019/001" }
  }
}
```

### Error responses

Errors include `_error_type` and `_meta`:

```json
{
  "error": "Decision not found: CCPC/E/2099/999",
  "_error_type": "not_found",
  "_meta": { ... }
}
```

Error types: `not_found`, `tool_error`, `unknown_tool`.
