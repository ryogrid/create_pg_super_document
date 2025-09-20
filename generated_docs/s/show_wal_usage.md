# show_wal_usage

## Location
[src/backend/commands/explain.c:3912-3949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3912-L3949)

## Overview
Displays Write-Ahead Logging (WAL) usage statistics in EXPLAIN output, showing WAL records generated, full page images, and total bytes written during query execution.

## Definition

```c
static void
show_wal_usage(ExplainState *es, const WalUsage *usage)
```
## Detailed Description
The  function formats and displays WAL usage statistics in EXPLAIN output. It provides insights into the Write-Ahead Logging activity generated during query execution, which is crucial for understanding the logging overhead and recovery implications of different query plans.

The function handles both text and structured output formats:
- **Text format**: Displays a compact "WAL: records=X fpi=Y bytes=Z" format, showing only non-zero values
- **Structured formats** (JSON/XML/YAML): Provides individual properties for each WAL statistic

The function tracks three key WAL metrics:
- **WAL records**: Number of WAL records generated
- **FPI (Full Page Images)**: Number of full page images written to WAL
- **WAL bytes**: Total number of bytes written to WAL

This information helps database administrators and developers understand the WAL generation characteristics of their queries, which affects both performance and storage requirements.

## Parameters / Member Variables
- : ExplainState structure containing output formatting context and destination string buffer
- : WalUsage structure containing WAL operation counters (records, full page images, bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainIndentText](../E/ExplainIndentText.md)
  - appendStringInfoString
  - appendStringInfo
  - appendStringInfoChar
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md)
  - [ExplainPropertyUInteger](../E/ExplainPropertyUInteger.md)
  - UINT64_FORMAT
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- Only displays positive counter values to avoid cluttering output with zeros
- WAL bytes are displayed using UINT64_FORMAT to handle large values correctly
- FPI (Full Page Images) represent complete page copies written to WAL during the first modification after a checkpoint
- Part of PostgreSQL's comprehensive EXPLAIN infrastructure for performance analysis
- WAL usage information is valuable for understanding query impact on transaction log size and write performance