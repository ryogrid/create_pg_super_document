# show_buffer_usage

## Location
[src/backend/commands/explain.c:3743-3911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3743-L3911)

## Overview
Displays detailed buffer usage statistics in EXPLAIN output, formatting information about shared, local, and temporary buffer operations for query performance analysis.

## Definition

```c
static void
show_buffer_usage(ExplainState *es, const BufferUsage *usage)
```
## Detailed Description
The  function is responsible for formatting and displaying buffer usage statistics in EXPLAIN output. It handles both text and non-text (JSON, XML, YAML) output formats, showing comprehensive information about buffer operations including:

- **Shared buffers**: Operations on shared buffer pool (hit, read, dirtied, written)
- **Local buffers**: Operations on backend-local buffers (hit, read, dirtied, written)  
- **Temporary buffers**: Operations on temporary buffers (read, written)
- **I/O timing**: Time spent on read and write operations for each buffer type

For text format, the function intelligently groups related statistics and only displays non-zero values. For structured formats (JSON/XML/YAML), it provides all statistics as individual properties. I/O timing information is only included when  is enabled.

The function must stay synchronized with  to ensure consistent buffer usage reporting across different contexts.

## Parameters / Member Variables
- `*es`: ExplainState structure containing output formatting context and destination string buffer
- `*usage`: BufferUsage structure containing all buffer operation counters and timing information to be displayed
## Dependencies
- Functions called/Symbols referenced:
  - [ExplainIndentText](../E/ExplainIndentText.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md)
  - [ExplainPropertyFloat](../E/ExplainPropertyFloat.md)
  - INSTR_TIME_IS_ZERO
  - INSTR_TIME_GET_MILLISEC
- Called from (representative examples):
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [ExplainPrintSerialize](../E/ExplainPrintSerialize.md)
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- Only displays positive counter values to avoid cluttering output with zeros
- Text format provides human-readable grouped output ("Buffers: shared hit=X read=Y...")
- Structured formats provide individual properties for programmatic consumption
- I/O timing display is conditional on the  configuration setting
- Must remain synchronized with  function for consistency
- Part of PostgreSQL's EXPLAIN infrastructure for query performance analysis

## Simplified Source

```c
static void show_buffer_usage(ExplainState *es, const BufferUsage *usage) {
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Text format: compact, grouped output
        bool has_shared = (usage->shared_blks_hit > 0 || /* other shared counters */ );
        bool has_local = (usage->local_blks_hit > 0 || /* other local counters */ );
        bool has_temp = (usage->temp_blks_read > 0 || usage->temp_blks_written > 0);

        // Display buffer counters
        if (has_shared || has_local || has_temp) {
            ExplainIndentText(es);
            appendStringInfoString(es->str, "Buffers:");

            // Show shared buffer stats
            if (has_shared) {
                appendStringInfoString(es->str, " shared");
                if (usage->shared_blks_hit > 0)
                    appendStringInfo(es->str, " hit=%lld", usage->shared_blks_hit);
                // ... other shared stats
            }

            // Show local and temp buffer stats similarly
            // ...

            appendStringInfoChar(es->str, '\n');
        }

        // Display I/O timing if present
        if (/* has timing info */) {
            ExplainIndentText(es);
            appendStringInfoString(es->str, "I/O Timings:");
            // Format timing information for shared/local/temp
            appendStringInfoChar(es->str, '\n');
        }
    } else {
        // Structured formats: individual properties
        ExplainPropertyInteger("Shared Hit Blocks", NULL, usage->shared_blks_hit, es);
        ExplainPropertyInteger("Shared Read Blocks", NULL, usage->shared_blks_read, es);
        // ... all other properties

        if (track_io_timing) {
            ExplainPropertyFloat("Shared I/O Read Time", "ms",
                               INSTR_TIME_GET_MILLISEC(usage->shared_blk_read_time), 3, es);
            // ... other timing properties
        }
    }
}
```