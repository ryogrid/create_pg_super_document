# get_update_query_targetlist_def

## Location
src/backend/utils/adt/ruleutils.c: 6919 - 7070

## Overview
Generates the SET clause portion of an UPDATE statement by deparsing the targetList, handling both simple assignments and complex multi-assignment scenarios.

## Definition
```c
static void get_update_query_targetlist_def(Query *query, List *targetList, deparse_context *context, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for generating the SET clause of UPDATE statements from PostgreSQL's internal targetList representation. It handles complex scenarios including:

1. **Simple assignments**: Standard column = value assignments
2. **Multi-assignments**: Tuple assignments like (col1, col2) = (subquery)
3. **Indirection**: Field access and array subscripts in assignments
4. **MULTIEXPR sublinks**: Subqueries returning multiple columns for tuple assignments

The function processes each TargetEntry in the targetList, filtering out resjunk entries, and formats them as comma-separated assignments. For multi-assignments, it collects MULTIEXPR_SUBLINK entries and groups related columns within parentheses.

Key processing steps:
- Collect MULTIEXPR sublinks for multi-column assignments
- Process each non-junk target entry
- Handle multi-assignment grouping with parentheses
- Resolve column names from system catalogs (not from resname)
- Process field/array indirection
- Generate appropriate assignment expressions

## Parameters / Member Variables
- `query`: The Query structure containing the UPDATE statement being deparsed
- `targetList`: List of TargetEntry nodes representing the SET clause assignments
- `context`: The deparse_context for formatting preferences and output buffer
- `rte`: RangeTblEntry for the target relation being updated (used for column name resolution)

## Dependencies
- Functions called/Symbols referenced:
  - list_head, lnext
  - [strip_implicit_coercions](../s/strip_implicit_coercions.md)
  - [count_nonjunk_tlist_entries](../c/count_nonjunk_tlist_entries.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [get_attname](get_attname.md)
  - processIndirection
  - get_rule_expr
- Called from:
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_update_query_def](get_update_query_def.md)
  - [get_merge_query_def](get_merge_query_def.md)

## Notes and Other Information
- This is a static function within ruleutils.c, part of PostgreSQL's rule decompilation system
- The function handles PostgreSQL's advanced multi-assignment syntax for tuple updates
- Column names are resolved from system catalogs to handle RENAME operations correctly
- MULTIEXPR sublinks are identified by their paramkind and processed specially for tuple assignments
- The function processes complex indirection patterns including nested field stores, array subscripts, and domain coercions
- Resjunk entries in the targetList are skipped as they are internal bookkeeping entries
- Part of the broader UPDATE statement deparsing infrastructure used across INSERT, UPDATE, and MERGE operations