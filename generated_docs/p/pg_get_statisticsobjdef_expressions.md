# pg_get_statisticsobjdef_expressions

## Location
src/backend/utils/adt/ruleutils.c: 1818 - 1888

## Overview
A PostgreSQL system function that extracts and returns the expressions from an extended statistics object as a text array, formatted for human readability.

## Definition
```c
Datum pg_get_statisticsobjdef_expressions(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that specifically handles the extraction and formatting of expressions from extended statistics objects. Unlike simple column-based statistics, extended statistics can include complex expressions that need to be properly deparsed and formatted. The function retrieves the stored expressions from the system catalog, deserializes them from their internal representation, and formats them into human-readable text using proper indentation and context-aware deparsing. The result is returned as a PostgreSQL text array, with each expression as a separate array element.

The function performs the following key operations:
1. Validates that the statistics object exists
2. Checks whether the statistics object contains expressions
3. Retrieves and deserializes the expression list from the catalog
4. Creates a proper deparse context for the relation
5. Formats each expression with appropriate pretty-printing
6. Builds and returns a text array containing all formatted expressions

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides:
  - `statextid`: The OID of the statistics object to examine (retrieved via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_statistic_ext (system catalog structure)
  - ArrayBuildState (array construction)
  - heap_attisnull, SysCacheGetAttrNotNull (catalog access)
  - TextDatumGetCString, stringToNode (expression deserialization)
  - deparse_context_for, get_relation_name (context setup)
  - deparse_expression_pretty (expression formatting)
  - accumArrayResult, cstring_to_text, makeArrayResult (array building)
  - PG_RETURN_DATUM (result return)
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL queries
- Returns NULL if the statistics object does not exist or has no expressions
- Uses PRETTYFLAG_INDENT for readable expression formatting
- Builds a PostgreSQL text array as the return type
- Handles memory management properly with pfree() calls
- The function is specifically designed for statistics objects that contain expressions (not just simple columns)
- Uses proper deparse context to ensure correct name resolution within expressions
- Part of the ruleutils module which handles object definition formatting
- Each expression in the result array is independently formatted and readable