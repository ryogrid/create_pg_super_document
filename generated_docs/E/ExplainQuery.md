# ExplainQuery

## Location
src/backend/commands/explain.c: 183 - 371

## Overview
ExplainQuery executes an EXPLAIN command by parsing options, rewriting queries, and generating execution plan output in various formats.

## Definition


## Detailed Description
ExplainQuery is the main entry point for processing EXPLAIN commands in PostgreSQL. It handles the complete workflow from parsing EXPLAIN options to generating the final output. The function creates an ExplainState object, parses all EXPLAIN options (analyze, verbose, costs, buffers, wal, settings, timing, memory, serialize, format), validates option combinations, rewrites the query using QueryRewrite, and then explains each resulting query plan. The output can be formatted as text, XML, JSON, or YAML depending on the FORMAT option.

The function performs extensive validation of option combinations, such as ensuring that WAL, TIMING, and SERIALIZE options are only used with ANALYZE, and that GENERIC_PLAN cannot be used with ANALYZE. After processing, it outputs the results through tuple output functions that handle multi-line or single-line formatting based on the chosen format.

## Parameters / Member Variables
- : ParseState containing parser context and source text information
- : ExplainStmt containing the query to explain and list of EXPLAIN options
- : ParamListInfo containing parameter values for parameterized queries
- : DestReceiver specifying where to send the explain output results

## Dependencies
- Functions called/Symbols referenced:
  - [NewExplainState](../N/NewExplainState.md)
  - [defGetBoolean](../d/defGetBoolean.md)/defGetString
  - [QueryRewrite](../Q/QueryRewrite.md)
  - [ExplainBeginOutput](ExplainBeginOutput.md)/ExplainEndOutput
  - [ExplainOneQuery](ExplainOneQuery.md)
  - [ExplainResultDesc](ExplainResultDesc.md)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)
  - [do_text_output_multiline](../d/do_text_output_multiline.md)/do_text_output_oneline
  - [end_tup_output](../e/end_tup_output.md)
  - [JumbleQuery](../J/JumbleQuery.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Supports multiple output formats: TEXT, XML, JSON, YAML
- Handles serialization options for plan data (NONE, TEXT, BINARY)  
- Enforces strict validation rules between different EXPLAIN options
- Uses post_parse_analyze_hook for extensibility if available
- Handles INSTEAD NOTHING rules by showing appropriate message
- Creates tuple descriptors for proper output formatting in different destinations