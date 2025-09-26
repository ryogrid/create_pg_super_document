# show_sort_group_keys

## Location
[src/backend/commands/explain.c:2759-2820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2759-L2820)

## Overview
A static function that provides common functionality for displaying sort and group keys in PostgreSQL's EXPLAIN output, handling the deparsing and formatting of key expressions with optional sort order information.

## Definition
```c
static void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
                     int nkeys, int nPresortedKeys, AttrNumber *keycols,
                     Oid *sortOperators, Oid *collations, bool *nullsFirst,
                     List *ancestors, ExplainState *es)
```

## Detailed Description
The `show_sort_group_keys` function is a central utility within PostgreSQL's EXPLAIN infrastructure that handles the display of sorting and grouping keys. It takes arrays of targetlist indexes that represent keys and converts them into human-readable expressions for EXPLAIN output. The function can handle both simple grouping keys (where sort-related parameters are NULL) and complex sorting keys (with operators, collations, and null ordering).

The function works by iterating through the provided key columns, finding the corresponding expressions in the plan's target list, deparsingthose expressions into readable text, and optionally appending sort order information. It also handles presorted keys separately, which are keys that are already in the correct order from earlier operations.

## Parameters / Member Variables
- `planstate`: The execution state of the plan node whose keys are being displayed
- `qlabel`: Label string to use when displaying the keys (e.g., "Sort Key", "Group Key")
- `nkeys`: Total number of keys to display
- `nPresortedKeys`: Number of keys that are already presorted (0 if not applicable)
- `keycols`: Array of AttrNumber values indicating which target list entries are keys
- `sortOperators`: Array of sort operator OIDs (NULL for grouping keys)
- `collations`: Array of collation OIDs (NULL for grouping keys)
- `nullsFirst`: Array of boolean values indicating null ordering (NULL for grouping keys)
- `ancestors`: List of ancestor plan nodes for context during deparsing
- `es`: ExplainState containing output formatting options

## Dependencies
- Functions called/Symbols referenced:
  - [set_deparse_context_plan](set_deparse_context_plan.md) (sets up context for expression deparsing)
  - [get_tle_by_resno](../g/get_tle_by_resno.md) (finds target entry by result number)
  - [deparse_expression](../d/deparse_expression.md) (converts expression nodes to readable text)
  - [show_sortorder_options](show_sortorder_options.md) (adds sort order information to expressions)
  - [resetStringInfo](../r/resetStringInfo.md) (resets string buffer)
  - [ExplainPropertyList](../E/ExplainPropertyList.md) (outputs the formatted key list)
- Types referenced:
  - [PlanState](../P/PlanState.md), Plan, TargetEntry, AttrNumber, ExplainState
- Called from (representative examples):
  - [show_sort_keys](show_sort_keys.md) (for Sort nodes)
  - [show_incremental_sort_keys](show_incremental_sort_keys.md) (for IncrementalSort nodes)
  - [show_merge_append_keys](show_merge_append_keys.md) (for MergeAppend nodes)
  - [show_agg_keys](show_agg_keys.md) (for Agg nodes)
  - [show_grouping_set_keys](show_grouping_set_keys.md) (for GroupingSets)
  - [show_group_keys](show_group_keys.md) (for Group nodes)

## Notes and Other Information
- The function returns early if nkeys <= 0, avoiding unnecessary processing
- Uses a StringInfoData buffer to efficiently build the formatted key expressions
- Determines whether to use table prefixes based on the number of tables in the query or verbose mode
- Handles presorted keys separately, displaying them under a "Presorted Key" label
- Error handling includes checking that target entries exist for all specified key column numbers
- The function is designed to be reusable across different types of plan nodes that use keys for sorting or grouping