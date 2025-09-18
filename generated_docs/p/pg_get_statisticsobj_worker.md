# pg_get_statisticsobj_worker

## Location
src/backend/utils/adt/ruleutils.c: 1634 - 1817

## Overview
The internal workhorse function that decompiles an extended statistics object into its textual definition, supporting various output modes for different use cases.

## Definition
```c
static char *pg_get_statisticsobj_worker(Oid statextid, bool columns_only, bool missing_ok)
```

## Detailed Description
This is the core function responsible for reconstructing the CREATE STATISTICS command text from a statistics object stored in the system catalogs. It handles the complex process of extracting statistics object metadata, decoding column references and expressions, determining enabled statistics types, and formatting the output appropriately. The function supports multiple output modes: full CREATE STATISTICS command generation, columns-only mode for inspection purposes, and flexible error handling when objects don't exist.

The function performs several key operations:
1. Retrieves statistics object metadata from pg_statistic_ext catalog
2. Extracts and deserializes statistics expressions if present
3. Decodes enabled statistics types (ndistinct, dependencies, mcv)
4. Formats column references and expressions with proper quoting
5. Constructs the appropriate output based on the requested mode

## Parameters / Member Variables
- `statextid`: The OID of the statistics object to decompile
- `columns_only`: If true, returns only the column/expression list; if false, returns the full CREATE STATISTICS command
- `missing_ok`: If true, returns NULL when the object doesn't exist; if false, throws an error

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_statistic_ext (system catalog structure)
  - [heap_attisnull](../h/heap_attisnull.md), SysCacheGetAttrNotNull (catalog access)
  - TextDatumGetCString, stringToNode (expression handling)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md), quote_qualified_identifier (name formatting)
  - DatumGetArrayTypeP, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE (array processing)
  - [get_attname](../g/get_attname.md), quote_identifier (column name handling)
  - [deparse_context_for](../d/deparse_context_for.md), deparse_expression_pretty (expression formatting)
  - generate_relation_name (relation name formatting)
  - looks_like_function (expression analysis)
- Called from (representative examples):
  - [pg_get_statisticsobjdef](pg_get_statisticsobjdef.md)
  - [pg_get_statisticsobjdef_string](pg_get_statisticsobjdef_string.md)
  - [pg_get_statisticsobjdef_columns](pg_get_statisticsobjdef_columns.md)

## Notes and Other Information
- This is a static (internal) function not directly accessible from SQL
- Handles complex logic for determining when to include statistics type clauses
- Supports both simple column references and complex expressions
- Uses sophisticated expression deparsing to maintain readability
- Implements proper memory management with StringInfo buffer
- The function omits type clauses when all statistics types are enabled to ensure forward compatibility
- For single-column statistics (expression statistics), type specification is unnecessary
- Properly handles the stxkind array to determine which statistics types are enabled
- Uses deparse context to ensure proper name resolution for expressions