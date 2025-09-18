# nameeqtext

## Location
src/backend/utils/adt/varlena.c: 2600 - 2624

## Overview
A cross-type equality comparison function that compares a name (fixed-length string) with a text (variable-length string) value for equality.

## Definition
Datum nameeqtext(PG_FUNCTION_ARGS)

## Detailed Description
This function implements cross-type equality comparison between PostgreSQL's name and text data types. The name type is a fixed-length string type used internally for system identifiers (like table names, column names), while text is a variable-length string type. The function performs collation-aware comparison, with an optimization for C collation that uses simple byte-wise comparison via memcmp. For other collations, it uses the more sophisticated varstr_cmp function to handle locale-specific comparison rules.

The function first checks if the lengths match, and if they do, performs the appropriate comparison based on the collation.

## Parameters / Member Variables
- arg1: Name value (fixed-length string) retrieved via PG_GETARG_NAME(0)
- arg2: Text value (variable-length string) retrieved via PG_GETARG_TEXT_PP(1)
- len1: Length of the name string (calculated using strlen)
- len2: Length of the text string (using VARSIZE_ANY_EXHDR)
- collid: Collation OID retrieved from the function context
- result: Boolean result of the equality comparison

## Dependencies
- Functions called/Symbols referenced:
  - Name (data type)
  - PG_GETARG_NAME
  - PG_GET_COLLATION
  - [check_collation_set](../c/check_collation_set.md)
  - [varstr_cmp](../v/varstr_cmp.md)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through SQL equality operators)

## Notes and Other Information
- Optimizes for C collation by using direct memory comparison (memcmp)
- Uses varstr_cmp for non-C collations to handle locale-specific comparison rules
- Part of the cross-type comparison functions between name and text types
- The function properly handles memory management with PG_FREE_IF_COPY for the text argument
- Essential for SQL operations that compare system identifiers (names) with user text values
- Located in src/backend/utils/adt/varlena.c:2600-2624