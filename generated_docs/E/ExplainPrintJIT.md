# ExplainPrintJIT

## Location
[src/backend/commands/explain.c:1011-1108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1011-L1108)

## Overview
Formats and appends JIT compilation information to EXPLAIN output, displaying JIT statistics, options, and timing details in both text and structured formats.

## Definition
```c
static void ExplainPrintJIT(ExplainState *es, int jit_flags, JitInstrumentation *ji)
```

## Detailed Description
ExplainPrintJIT is responsible for formatting and displaying JIT (Just-In-Time) compilation statistics in EXPLAIN query plans. The function supports multiple output formats (text, XML, JSON, YAML) and provides detailed information about JIT compilation phases including generation, inlining, optimization, and emission.

The function calculates total JIT time by aggregating individual phase timers (excluding deform_counter which is included in generation_counter). It displays the number of JIT-compiled functions, enabled JIT options, and detailed timing information when analysis and timing are enabled.

For text format, it produces a compact, human-readable output. For structured formats (XML/JSON/YAML), it uses nested groups with proper property formatting.

## Parameters / Member Variables
- `es`: ExplainState structure containing formatting configuration and output destination
- `jit_flags`: Bitmask indicating which JIT features were enabled (inlining, optimization, expressions, deforming)
- `ji`: JitInstrumentation structure containing timing and statistics data from JIT compilation

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainOpenGroup](ExplainOpenGroup.md)/ExplainCloseGroup (structured output grouping)
  - [ExplainPropertyInteger](ExplainPropertyInteger.md)/ExplainPropertyBool/ExplainPropertyFloat (property formatting)
  - [ExplainIndentText](ExplainIndentText.md) (text formatting)
  - INSTR_TIME_* macros (timing manipulation)
  - PGJIT_* flags (JIT option constants)
- Called from (representative examples):
  - [ExplainPrintJITSummary](ExplainPrintJITSummary.md) (for aggregated JIT statistics)
  - [ExplainNode](ExplainNode.md) (for individual node JIT statistics)

## Notes and Other Information
- Static function, only accessible within explain.c
- Returns early if no JIT compilation occurred (created_functions == 0)
- Timing information only displayed when es->analyze and es->timing are enabled
- Converts timing from instr_time to milliseconds for display
- Handles both single-line text format and structured multi-property formats
- Part of PostgreSQLs comprehensive EXPLAIN infrastructure
- Located in src/backend/commands/explain.c:1011-1108

## Simplified Source

```c
static void
ExplainPrintJIT(ExplainState *es, int jit_flags, JitInstrumentation *ji)
{
    instr_time total_time;

    // Skip if no JIT compilation occurred
    if (!ji || ji->created_functions == 0)
        return;

    // Calculate total JIT time (excluding deform which is in generation)
    INSTR_TIME_SET_ZERO(total_time);
    INSTR_TIME_ADD(total_time, ji->generation_counter);
    INSTR_TIME_ADD(total_time, ji->inlining_counter);
    INSTR_TIME_ADD(total_time, ji->optimization_counter);
    INSTR_TIME_ADD(total_time, ji->emission_counter);

    ExplainOpenGroup("JIT", "JIT", true, es);

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Text format: compact output
        ExplainIndentText(es);
        appendStringInfoString(es->str, "JIT:\n");
        es->indent++;

        ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

        // Show JIT options
        ExplainIndentText(es);
        appendStringInfo(es->str, "Options: %s %s, %s %s, %s %s, %s %s\n",
                        "Inlining", jit_flags & PGJIT_INLINE ? "true" : "false",
                        "Optimization", jit_flags & PGJIT_OPT3 ? "true" : "false",
                        "Expressions", jit_flags & PGJIT_EXPR ? "true" : "false",
                        "Deforming", jit_flags & PGJIT_DEFORM ? "true" : "false");

        // Show timing if enabled
        if (es->analyze && es->timing) {
            ExplainIndentText(es);
            appendStringInfo(es->str,
                           "Timing: Generation %.3f ms (Deform %.3f ms), "
                           "Inlining %.3f ms, Optimization %.3f ms, "
                           "Emission %.3f ms, Total %.3f ms\n",
                           1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
                           1000.0 * INSTR_TIME_GET_DOUBLE(ji->deform_counter),
                           1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
                           1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
                           1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
                           1000.0 * INSTR_TIME_GET_DOUBLE(total_time));
        }
        es->indent--;
    } else {
        // Structured format: use property groups
        ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

        // JIT options group
        ExplainOpenGroup("Options", "Options", true, es);
        ExplainPropertyBool("Inlining", jit_flags & PGJIT_INLINE, es);
        ExplainPropertyBool("Optimization", jit_flags & PGJIT_OPT3, es);
        ExplainPropertyBool("Expressions", jit_flags & PGJIT_EXPR, es);
        ExplainPropertyBool("Deforming", jit_flags & PGJIT_DEFORM, es);
        ExplainCloseGroup("Options", "Options", true, es);

        // Timing group if enabled
        if (es->analyze && es->timing) {
            ExplainOpenGroup("Timing", "Timing", true, es);

            ExplainPropertyFloat("Generation", "ms",
                               1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter), 3, es);
            ExplainPropertyFloat("Inlining", "ms",
                               1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter), 3, es);
            ExplainPropertyFloat("Optimization", "ms",
                               1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter), 3, es);
            ExplainPropertyFloat("Emission", "ms",
                               1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter), 3, es);
            ExplainPropertyFloat("Total", "ms",
                               1000.0 * INSTR_TIME_GET_DOUBLE(total_time), 3, es);

            ExplainCloseGroup("Timing", "Timing", true, es);
        }
    }

    ExplainCloseGroup("JIT", "JIT", true, es);
}
```