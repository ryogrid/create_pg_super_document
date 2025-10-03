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
- `*es`: ExplainState structure containing output formatting context and destination string buffer
- `*usage`: WalUsage structure containing WAL operation counters (records, full page images, bytes)
## Dependencies
- Functions called/Symbols referenced:
  - [ExplainIndentText](../E/ExplainIndentText.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
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

## Simplified Source

```c
static void
show_wal_usage(ExplainState *es, const WalUsage *usage)
{
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Show only positive values in compact format
        if (usage->wal_records > 0 || usage->wal_fpi > 0 || usage->wal_bytes > 0) {
            ExplainIndentText(es);
            appendStringInfoString(es->str, "WAL:");

            if (usage->wal_records > 0)
                appendStringInfo(es->str, " records=%lld", (long long) usage->wal_records);
            if (usage->wal_fpi > 0)
                appendStringInfo(es->str, " fpi=%lld", (long long) usage->wal_fpi);
            if (usage->wal_bytes > 0)
                appendStringInfo(es->str, " bytes=" UINT64_FORMAT, usage->wal_bytes);

            appendStringInfoChar(es->str, '\n');
        }
    } else {
        // Structured output format (JSON/XML/YAML)
        ExplainPropertyInteger("WAL Records", NULL, usage->wal_records, es);
        ExplainPropertyInteger("WAL FPI", NULL, usage->wal_fpi, es);
        ExplainPropertyUInteger("WAL Bytes", NULL, usage->wal_bytes, es);
    }
}
```