# show_memory_counters

## Location
[src/backend/commands/explain.c:3950-3975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3950-L3975)

## Overview
Displays memory usage statistics in EXPLAIN output, showing both used and allocated memory in kilobytes for memory context analysis.

## Definition


## Detailed Description
The  function formats and displays memory usage statistics in EXPLAIN output. It provides insights into memory consumption patterns during query execution, which is essential for understanding memory-intensive operations and potential memory-related performance bottlenecks.

The function calculates and displays two key memory metrics:
- **Memory Used**: Actual memory in use (total allocated - free space)
- **Memory Allocated**: Total memory allocated by the memory context system

Both values are converted from bytes to kilobytes using the BYTES_TO_KILOBYTES macro for more readable output. The function handles both text and structured output formats:
- **Text format**: Displays as "Memory: used=XkB allocated=YkB"
- **Structured formats** (JSON/XML/YAML): Provides individual properties with "kB" units

This information helps identify queries that consume significant memory and aids in capacity planning and performance tuning.

## Parameters / Member Variables
- : ExplainState structure containing output formatting context and destination string buffer
- : MemoryContextCounters structure containing memory statistics (totalspace, freespace)

## Dependencies
- Functions called/Symbols referenced:
  - BYTES_TO_KILOBYTES
  - [ExplainIndentText](../E/ExplainIndentText.md)
  - appendStringInfo
  - appendStringInfoChar
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md)
  - INT64_FORMAT
- Called from (representative examples):
  - [ExplainOnePlan](../E/ExplainOnePlan.md)

## Notes and Other Information
- Memory values are automatically converted from bytes to kilobytes for better readability
- Used memory is calculated as (totalspace - freespace) to show actual memory consumption
- Part of PostgreSQL's memory context system for tracking memory allocations
- Helps identify memory-intensive query operations and potential memory leaks
- Always displays both used and allocated memory, unlike buffer/WAL usage which filters zero values
- Memory statistics are particularly useful for analyzing hash joins, sorts, and other memory-intensive operations