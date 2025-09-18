# texteqname

## Location
src/backend/utils/adt/varlena.c: 2625 - 2649

## Overview
A cross-type equality comparison function that compares a text (variable-length string) with a name (fixed-length string) value for equality.

## Definition
Datum texteqname(PG_FUNCTION_ARGS)

## Detailed Description
This function implements cross-type equality comparison between PostgreSQL's text and name data types. It is the complement to nameeqtext, with the argument order reversed - the first argument is text and the second is name. Like its counterpart, it performs collation-aware comparison with an optimization for C collation using simple byte-wise comparison via memcmp. For other collations, it uses varstr_cmp to handle locale-specific comparison rules.

The function follows the same logic pattern as nameeqtext: check lengths for equality first, then perform the appropriate comparison based on the collation.

## Parameters / Member Variables
- arg1: Text value (variable-length string) retrieved via PG_GETARG_TEXT_PP(0)
- arg2: Name value (fixed-length string) retrieved via PG_GETARG_NAME(1)
- len1: Length of the text string (using VARSIZE_ANY_EXHDR)
- len2: Length of the name string (calculated using strlen)
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
- Complement function to nameeqtext with reversed argument order
- Optimizes for C collation by using direct memory comparison (memcmp)
- Uses varstr_cmp for non-C collations to handle locale-specific comparison rules
- Part of the cross-type comparison functions between text and name types
- The function properly handles memory management with PG_FREE_IF_COPY for the text argument
- Essential for SQL operations that compare user text values with system identifiers (names)
- Located in src/backend/utils/adt/varlena.c:2625-2649