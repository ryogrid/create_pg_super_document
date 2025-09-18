# slot_fill_defaults

## Location
src/backend/replication/logical/worker.c: 742 - 798

## Overview
Evaluates and applies default values for columns in the downstream table that cannot be mapped to columns in the upstream (remote) table, enabling support for tables with more columns downstream than upstream.

## Definition
```c
static void
slot_fill_defaults(LogicalRepRelMapEntry *rel, EState *estate,
                   TupleTableSlot *slot)
```

## Detailed Description
This function handles schema evolution scenarios where the downstream (subscriber) table has more columns than the upstream (publisher) table during logical replication. It identifies columns that exist locally but not in the remote relation and fills them with appropriate default values.

The function works by:
1. Comparing the number of physical attributes between local and remote relations
2. Iterating through all local columns to identify those without remote mappings
3. Building default expressions for unmapped columns using build_column_default()
4. Planning and initializing the default expressions for execution
5. Evaluating the default expressions and storing results in the tuple slot

The function skips dropped columns and generated columns, and only processes columns that don't have a corresponding mapping in the remote relation (indicated by negative values in the attribute map).

## Parameters / Member Variables
- `rel`: LogicalRepRelMapEntry containing the mapping between local and remote relations, including the attribute mapping information
- `estate`: EState providing the executor context and expression evaluation environment
- `slot`: TupleTableSlot where the default values will be stored for unmapped columns

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - TupleDescAttr
  - [build_column_default](../b/build_column_default.md)
  - [expression_planner](../e/expression_planner.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - ExecEvalExpr
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md)

## Notes and Other Information
- This is a static function used internally within the logical replication worker
- The function supports schema evolution where downstream tables have additional columns not present upstream
- Early return optimization: if the number of physical attributes matches the remote relation attributes, no defaults are needed
- The function handles three types of columns that are skipped: dropped columns, generated columns, and columns that have valid remote mappings
- Default expressions are run through the planner for optimization before execution
- Uses per-tuple expression context for efficient expression evaluation
- Memory for defmap and defexprs arrays is allocated based on the total number of physical attributes
- The function modifies the slot's tts_values and tts_isnull arrays directly for unmapped columns
- Critical for maintaining data integrity when subscriber tables have evolved beyond publisher schema