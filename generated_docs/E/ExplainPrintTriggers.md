# ExplainPrintTriggers

## Location
src/backend/commands/explain.c: 942 - 984

## Overview
ExplainPrintTriggers converts trigger execution statistics from a QueryDesc into formatted output and appends it to the EXPLAIN results during ANALYZE operations.

## Definition


## Detailed Description
ExplainPrintTriggers generates a summary of trigger execution statistics collected during query execution when EXPLAIN ANALYZE is used. The function examines three different types of result relations that may have had triggers executed:

1. **Regular result relations** (es_opened_result_relations): Standard target tables for INSERT/UPDATE/DELETE operations
2. **Tuple routing relations** (es_tuple_routing_result_relations): Partition tables involved in tuple routing for partitioned table operations  
3. **Trigger target relations** (es_trig_target_relations): Relations that were trigger targets but may not have been direct query targets

For each relation type, the function iterates through all relations and calls report_triggers() to output detailed trigger statistics. The function intelligently determines whether to show relation names based on whether multiple relations are involved in the operation.

The output is wrapped in a "Triggers" group for structured formats (JSON, XML, YAML) or appears as trigger timing information in text format.

## Parameters / Member Variables
- : ExplainState containing output formatting options and buffer for trigger statistics
- : QueryDesc containing the execution state with trigger execution statistics collected during query execution

## Dependencies
- Functions called/Symbols referenced:
  - ExplainOpenGroup
  - ExplainCloseGroup  
  - report_triggers
  - list_length
  - lfirst
- Called from (representative examples):
  - ExplainOnePlan

## Notes and Other Information
- This function is only called during EXPLAIN ANALYZE operations when triggers have actually been executed
- The show_relname flag is set when multiple relations are involved, ensuring clear attribution of trigger statistics to specific tables
- The function handles all three types of result relations that can have triggers in PostgreSQL's execution system
- Trigger statistics include timing information, number of calls, and potentially other execution metrics depending on the instrumentation level
- The function assumes that trigger instrumentation was enabled during query execution to collect meaningful statistics
- Empty result lists are handled gracefully - the function will output an empty triggers section if no triggers were executed