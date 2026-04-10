# Corpus Coverage

This document describes the completeness and scope of the data in this MCP server.

## Data Authority

**CCPC — Competition and Consumer Protection Commission**
Official Irish regulatory authority for competition law enforcement and merger control.
Website: https://www.ccpc.ie/

## Coverage Scope

### Enforcement Decisions

| Category | Coverage |
|----------|----------|
| Abuse of dominance (Section 5, Competition Act 2002) | CCPC published decisions |
| Cartel enforcement (Section 4, Competition Act 2002) | CCPC published decisions |
| Sector inquiries | CCPC published reports |
| Criminal cartel referrals | Summary metadata only |

### Merger Control Notifications

| Category | Coverage |
|----------|----------|
| Phase I clearances | CCPC published determinations |
| Phase II investigations | CCPC published determinations |
| Conditional clearances | CCPC published determinations |
| Prohibitions | CCPC published determinations |

## Known Limitations

- **Lag**: The database is updated periodically and may not reflect the most recent CCPC publications. Use `ie_comp_check_data_freshness` to check the last update timestamp.
- **Informal resolutions**: Matters resolved informally without a formal published decision may not be captured.
- **Pre-2000 decisions**: Coverage of decisions predating the current CCPC mandate may be incomplete.
- **Confidential redactions**: Where CCPC publishes redacted versions, only publicly available text is indexed.

## Completeness Statement

This corpus is a **research aid**, not a definitive or exhaustive registry. Always verify decisions against the primary CCPC source at https://www.ccpc.ie/ before relying on them for compliance purposes.

## Update Frequency

Database updates are triggered via the [ingest workflow](.github/workflows/ingest.yml), which runs weekly. The [freshness check workflow](.github/workflows/check-freshness.yml) monitors data age.
