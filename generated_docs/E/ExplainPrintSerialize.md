# ExplainPrintSerialize

## Location
src/backend/commands/explain.c: 1109 - 1168

## Overview
Formats and appends query output serialization metrics to EXPLAIN output, displaying serialization timing, output volume, and format information.

## Definition
```c
static void ExplainPrintSerialize(ExplainState *es, SerializeMetrics *metrics)
```

## Detailed Description
ExplainPrintSerialize displays metrics related to query result serialization in EXPLAIN output. It shows information about the time spent serializing query results, the volume of data serialized, the serialization format used (text or binary), and optionally buffer usage statistics.

The function determines the serialization format from the ExplainState configuration and formats the output appropriately for both text and structured formats. When timing is enabled, it includes the time spent on serialization. When buffer statistics are enabled, it displays detailed buffer usage information with proper indentation for text format.

The function handles two serialization formats: text and binary, and should not be called when serialization is disabled (EXPLAIN_SERIALIZE_NONE).

## Parameters / Member Variables
- `es`: ExplainState structure containing formatting configuration and output settings
- `metrics`: SerializeMetrics structure containing serialization timing, byte counts, and buffer usage statistics

## Dependencies
- Functions called/Symbols referenced:
  - ExplainOpenGroup/ExplainCloseGroup (structured output grouping)
  - ExplainPropertyFloat/ExplainPropertyUInteger/ExplainPropertyText (property formatting)
  - ExplainIndentText (text formatting)
  - show_buffer_usage/peek_buffer_usage (buffer statistics display)
  - INSTR_TIME_GET_DOUBLE (timing conversion)
  - BYTES_TO_KILOBYTES (unit conversion)
  - EXPLAIN_SERIALIZE_* constants (serialization format flags)
- Called from (representative examples):
  - ExplainOnePlan (main EXPLAIN plan processing function)

## Notes and Other Information
- Static function, only accessible within explain.c
- Should not be called when es->serialize is EXPLAIN_SERIALIZE_NONE
- Supports both text and binary serialization format reporting
- Timing information only displayed when es->timing is enabled
- Buffer usage information only displayed when es->buffers is enabled
- Converts timing from instr_time to milliseconds and bytes to kilobytes for display
- Part of PostgreSQLs EXPLAIN infrastructure for displaying query execution details
- Located in src/backend/commands/explain.c:1109-1168