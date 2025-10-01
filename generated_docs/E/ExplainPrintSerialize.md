# ExplainPrintSerialize

## Location
[src/backend/commands/explain.c:1109-1168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1109-L1168)

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
  - [ExplainOpenGroup](ExplainOpenGroup.md)/ExplainCloseGroup (structured output grouping)
  - [ExplainPropertyFloat](ExplainPropertyFloat.md)/ExplainPropertyUInteger/ExplainPropertyText (property formatting)
  - [ExplainIndentText](ExplainIndentText.md) (text formatting)
  - [show_buffer_usage](../s/show_buffer_usage.md)/peek_buffer_usage (buffer statistics display)
  - INSTR_TIME_GET_DOUBLE (timing conversion)
  - BYTES_TO_KILOBYTES (unit conversion)
  - EXPLAIN_SERIALIZE_* constants (serialization format flags)
- Called from (representative examples):
  - [ExplainOnePlan](ExplainOnePlan.md) (main EXPLAIN plan processing function)

## Notes and Other Information
- Static function, only accessible within explain.c
- Should not be called when es->serialize is EXPLAIN_SERIALIZE_NONE
- Supports both text and binary serialization format reporting
- Timing information only displayed when es->timing is enabled
- Buffer usage information only displayed when es->buffers is enabled
- Converts timing from instr_time to milliseconds and bytes to kilobytes for display
- Part of PostgreSQLs EXPLAIN infrastructure for displaying query execution details
- Located in src/backend/commands/explain.c:1109-1168

## Simplified Source

```c
static void ExplainPrintSerialize(ExplainState *es, SerializeMetrics *metrics)
{
    const char *format;

    // Determine serialization format
    if (es->serialize == EXPLAIN_SERIALIZE_TEXT)
        format = "text";
    else {
        Assert(es->serialize == EXPLAIN_SERIALIZE_BINARY);
        format = "binary";
    }

    ExplainOpenGroup("Serialization", "Serialization", true, es);

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Text format output
        ExplainIndentText(es);
        if (es->timing)
            appendStringInfo(es->str, "Serialization: time=%.3f ms  output=" UINT64_FORMAT "kB  format=%s\n",
                           1000.0 * INSTR_TIME_GET_DOUBLE(metrics->timeSpent),
                           BYTES_TO_KILOBYTES(metrics->bytesSent), format);
        else
            appendStringInfo(es->str, "Serialization: output=" UINT64_FORMAT "kB  format=%s\n",
                           BYTES_TO_KILOBYTES(metrics->bytesSent), format);

        // Show buffer usage if enabled
        if (es->buffers && peek_buffer_usage(es, &metrics->bufferUsage)) {
            es->indent++;
            show_buffer_usage(es, &metrics->bufferUsage);
            es->indent--;
        }
    } else {
        // Structured format output
        if (es->timing)
            ExplainPropertyFloat("Time", "ms",
                               1000.0 * INSTR_TIME_GET_DOUBLE(metrics->timeSpent), 3, es);
        ExplainPropertyUInteger("Output Volume", "kB",
                              BYTES_TO_KILOBYTES(metrics->bytesSent), es);
        ExplainPropertyText("Format", format, es);
        if (es->buffers)
            show_buffer_usage(es, &metrics->bufferUsage);
    }

    ExplainCloseGroup("Serialization", "Serialization", true, es);
}
```