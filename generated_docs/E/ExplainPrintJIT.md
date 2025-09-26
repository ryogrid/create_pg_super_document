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