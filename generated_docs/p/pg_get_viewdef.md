# pg_get_viewdef

## Location
src/backend/utils/adt/ruleutils.c: 657 - 675

## Overview
PostgreSQL SQL function that returns the SELECT statement definition of a view in text format suitable for recreation.

## Definition
```c
Datum pg_get_viewdef(PG_FUNCTION_ARGS)
```

## Detailed Description
This function extracts and returns the SELECT portion of a view definition as formatted SQL text. Unlike rule definitions which include the complete CREATE RULE statement, this function focuses specifically on the query part of view definitions. It uses default pretty-printing with indentation and standard column wrapping to ensure readable output format.

The function serves as a public interface for view introspection, commonly used by database administration tools and pg_dump for view recreation.

## Parameters / Member Variables
- `viewoid`: OID of the view to retrieve the definition for (obtained via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_viewdef_worker](pg_get_viewdef_worker.md) - Core worker function for view definition generation
  - `string_to_text` - Converts C string to PostgreSQL text type
  - `PRETTYFLAG_INDENT` - Constant for formatting with indentation
  - `WRAP_COLUMN_DEFAULT` - Default column width for line wrapping
  - `PG_RETURN_TEXT_P` - Macro for returning text result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase (likely called via SQL function interface)

## Notes and Other Information
- Located at src/backend/utils/adt/ruleutils.c:657-675
- Returns NULL if the view definition cannot be retrieved
- Uses fixed formatting parameters for consistent output
- Focuses on the SELECT query rather than the complete view definition
- Part of PostgreSQL's system information functions for database introspection