# report_triggers

## Location
src/backend/commands/explain.c: 1202 - 1272

## Overview
Generates execution statistics report for triggers associated with a single relation, including timing information and call counts when available.

## Definition


## Detailed Description
The  function is responsible for reporting execution statistics for all triggers associated with a specific relation during query execution. It iterates through all triggers defined on the relation and outputs performance metrics including execution time and number of calls. The function only reports triggers that were actually invoked during the query execution, ignoring those that were never triggered as they are likely not relevant to the current query type.

The output format depends on the explain format specified in the ExplainState. For text format, it provides a compact representation with optional verbose details, while non-text formats (JSON, XML, YAML) include all available information in a structured manner.

## Parameters / Member Variables
- : ResultRelInfo structure containing relation information and trigger instrumentation data
- : Boolean flag indicating whether to include the relation name in the output
- : ExplainState structure containing formatting options and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - InstrEndLoop
  - ExplainOpenGroup
  - RelationGetRelationName
  - get_constraint_name
  - ExplainPropertyText
  - ExplainPropertyFloat
  - ExplainCloseGroup
- Called from (representative examples):
  - ExplainPrintTriggers

## Notes and Other Information
- The function cleans up instrumentation state for each trigger using InstrEndLoop
- Triggers with zero tuple counts (never invoked) are skipped in the output
- For constraint triggers, both trigger name and constraint name may be displayed
- Timing information is only included when es->timing is enabled
- Memory allocated for constraint names is properly freed using pfree
- Output formatting varies significantly between text and structured formats