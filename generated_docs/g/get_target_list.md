# get_target_list

## Location
src/backend/utils/adt/ruleutils.c: 6035 - 6170

## Overview
Parses back a SELECT target list into SQL text format, also used for RETURNING lists in INSERT/UPDATE/DELETE/MERGE statements.

## Definition
```c
static void get_target_list(List *targetList, deparse_context *context)
```

## Detailed Description
This function converts a list of TargetEntry nodes back into SQL text representation. It handles each target list entry by determining the appropriate column expression and alias. The function has special handling for Var nodes to avoid expanding whole-row variables into multiple columns at the top level of a SELECT list.

Key features include:
- Skips junk entries (internal entries not visible in SQL output)
- Special-cases Var nodes to prevent inappropriate expansion of whole-row variables
- Determines appropriate column names from view descriptors or TargetEntry names
- Adds AS clauses when necessary to preserve column naming
- Handles line wrapping and formatting based on context settings
- Manages comma separation between target list items

The function uses a temporary buffer to format each target entry before deciding on line wrapping, ensuring proper SQL formatting.

## Parameters / Member Variables
- `targetList`: List of TargetEntry nodes representing the SELECT target list or RETURNING clause
- `context`: deparse_context containing formatting options, output buffer, and view information

## Dependencies
- Functions called/Symbols referenced:
  - [get_variable](get_variable.md) (get text for Var nodes with proper whole-row handling)
  - get_rule_expr (get text for general expression nodes)
  - [quote_identifier](../q/quote_identifier.md) (properly quote SQL identifiers)
  - resetStringInfo (clear temporary string buffer)
  - removeStringInfoSpaces (formatting utility)
  - appendContextKeyword (add keywords with proper indentation)
  - appendBinaryStringInfo (append formatted text to output buffer)
- Called from (representative examples):
  - [get_basic_select_query](get_basic_select_query.md) (src/backend/utils/adt/ruleutils.c:5960)
  - [get_insert_query_def](get_insert_query_def.md) (src/backend/utils/adt/ruleutils.c:6853)
  - [get_update_query_def](get_update_query_def.md) (src/backend/utils/adt/ruleutils.c:6909)
  - [get_delete_query_def](get_delete_query_def.md) (src/backend/utils/adt/ruleutils.c:7112)
  - [get_merge_query_def](get_merge_query_def.md) (src/backend/utils/adt/ruleutils.c:7275)

## Notes and Other Information
- Critical component of PostgreSQL's rule decompilation system
- Handles view column renaming by using resultDesc when available
- Prevents whole-row Var expansion that would change query semantics at SELECT list level
- Supports intelligent line wrapping for better SQL readability
- Used across multiple statement types (SELECT, INSERT, UPDATE, DELETE, MERGE) for consistent target list formatting
- Manages AS clause generation to preserve original column naming intent