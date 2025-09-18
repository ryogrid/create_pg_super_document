# rewriteTargetListIU

## Location
src/backend/rewrite/rewriteHandler.c: 764 - 1035

## Overview
Rewrites INSERT/UPDATE target lists into standard form by handling defaults, merging multiple entries for the same attribute, and sorting into canonical order.

## Definition
```c
static List *rewriteTargetListIU(List *targetList, CmdType commandType, OverridingKind override, Relation target_relation, RangeTblEntry *values_rte, int values_rte_index, Bitmapset **unused_values_attrnos)
```

## Detailed Description
rewriteTargetListIU is a comprehensive function that transforms INSERT and UPDATE target lists to ensure proper handling of defaults, constraints, and PostgreSQL-specific features. The function performs three critical responsibilities:

1. **Default Value Processing**: For INSERT operations, adds target list entries to compute default values for any attributes that have defaults but are not explicitly assigned. Replaces explicit DEFAULT specifications with actual column default expressions for both INSERT and UPDATE operations.

2. **Multiple Entry Merging**: Handles cases where the same target attribute appears multiple times, such as partial array or record field updates (e.g., `SET foo[2] = 42, foo[4] = 43`). These are merged into single assignment operations using functions like array_set_element.

3. **Target List Sorting**: Sorts the target list into standard order with non-junk fields ordered by resno, followed by junk fields in arbitrary order.

The function also handles special column types:
- **Identity Columns**: Enforces GENERATED ALWAYS constraints and handles OVERRIDING clauses
- **Generated Columns**: Ensures only DEFAULT values can be inserted/updated into generated columns
- **VALUES RTE Integration**: For multi-row INSERTs using VALUES, tracks which VALUES columns become unused when replaced with defaults

## Parameters / Member Variables
- `targetList`: The original target list to be rewritten
- `commandType`: Command type (CMD_INSERT or CMD_UPDATE)
- `override`: Overriding clause specification (for identity columns)
- `target_relation`: The target relation being modified
- `values_rte`: Range table entry for VALUES clause (NULL if not applicable)
- `values_rte_index`: Index of the VALUES RTE in the range table
- `unused_values_attrnos`: Output parameter for tracking unused VALUES columns

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - [process_matched_tle](../p/process_matched_tle.md)
  - [flatCopyTargetEntry](../f/flatCopyTargetEntry.md)
  - [findDefaultOnlyColumns](../f/findDefaultOnlyColumns.md)
  - [build_column_default](../b/build_column_default.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [RewriteQuery](../R/RewriteQuery.md) (multiple call sites)

## Notes and Other Information
- This is a static function, only accessible within rewriteHandler.c
- Uses an O(N) algorithm with temporary array to avoid O(N^2) behavior for large attribute counts
- Critical for proper rule rewriting as it must be completed before firing rewrite rules
- Handles complex PostgreSQL features like identity columns (GENERATED ALWAYS/BY DEFAULT)
- Enforces constraints on generated columns that can only accept DEFAULT values
- Optimizes by omitting NULL defaults for INSERT operations (planner handles these)
- For UPDATE operations, explicitly sets NULL values when no default exists
- Properly handles junk attributes (ORDER BY, GROUP BY expressions) by assigning them resnos above real attributes
- Integrates with VALUES RTE processing to track columns that become unused due to default replacement