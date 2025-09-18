# show_modifytable_info

## Location
src/backend/commands/explain.c: 4172 - 4383

## Overview
show_modifytable_info is a static function that displays detailed information for ModifyTable nodes in PostgreSQL's EXPLAIN output, including target tables, foreign data wrapper details, and conflict resolution information.

## Definition
```c
static void show_modifytable_info(ModifyTableState *mtstate, List *ancestors, ExplainState *es)
```

## Detailed Description
This function serves three main objectives for ModifyTable operations (INSERT, UPDATE, DELETE, MERGE) in EXPLAIN output: (1) identify actual target tables when there are multiple targets or they differ from the nominal target, (2) allow foreign data wrappers (FDWs) to display additional information about foreign targets, and (3) show information about ON CONFLICT handling and MERGE operation statistics. The function handles complex scenarios like partitioned tables, foreign tables, and provides detailed instrumentation data when EXPLAIN ANALYZE is used.

## Parameters / Member Variables
- `mtstate`: Pointer to the ModifyTableState containing execution state information
- `ancestors`: List of ancestor plan nodes for context in qualification display
- `es`: Pointer to ExplainState structure controlling output format and options

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainOpenGroup](../E/ExplainOpenGroup.md)
  - [ExplainCloseGroup](../E/ExplainCloseGroup.md)
  - [ExplainIndentText](../E/ExplainIndentText.md)
  - [ExplainTargetRel](../E/ExplainTargetRel.md)
  - [ExplainPropertyText](../E/ExplainPropertyText.md)
  - [ExplainPropertyList](../E/ExplainPropertyList.md)
  - [ExplainPropertyFloat](../E/ExplainPropertyFloat.md)
  - [show_upper_qual](show_upper_qual.md)
  - [show_instrumentation_count](show_instrumentation_count.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [list_nth](../l/list_nth.md)
  - [InstrEndLoop](../I/InstrEndLoop.md)
  - outerPlanState
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Handles all DML operation types: INSERT, UPDATE, DELETE, and MERGE
- Provides special handling for ON CONFLICT clauses in INSERT operations
- Shows detailed tuple statistics for MERGE operations when EXPLAIN ANALYZE is used
- Supports foreign data wrapper integration through ExplainForeignModify callbacks
- Automatically labels target tables when there are multiple targets or they differ from nominal targets
- Provides comprehensive instrumentation data including conflict resolution statistics and tuple counts
- Part of PostgreSQL's advanced query execution plan explanation system