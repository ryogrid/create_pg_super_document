# pg_get_partition_constraintdef

## Location
[src/backend/utils/adt/ruleutils.c:2076-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2076-L2107)

## Overview
PostgreSQL function that returns the partition constraint expression as a formatted string for a given partitioned relation.

## Definition

```c
Datum
pg_get_partition_constraintdef(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a SQL-callable interface to retrieve the partition constraint definition for a relation. It takes a relation OID as input and returns the constraint expression that defines the partition boundaries as a formatted text string. The function retrieves the internal partition constraint expression using  and then deparses it into human-readable SQL syntax. If no partition constraint exists for the given relation, the function returns NULL. This is particularly useful for examining the logical constraints that determine which rows belong to a specific partition.

## Parameters / Member Variables
- Function expects one argument via :
  -  (Oid): Object identifier of the partitioned relation whose constraint definition should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID, PG_RETURN_NULL, PG_RETURN_TEXT_P (PostgreSQL function interface macros)
  - [get_partition_qual_relid](../g/get_partition_qual_relid.md) (retrieves the partition constraint expression)
  - [deparse_context_for](../d/deparse_context_for.md), get_relation_name (context setup for deparsing)
  - [deparse_expression_pretty](../d/deparse_expression_pretty.md) (converts expression tree to formatted SQL string)
  - string_to_text (converts C string to PostgreSQL text type)
  - PRETTYFLAG_INDENT (formatting constant for indented output)
- Called from (representative examples):
  - No direct references found in the codebase (likely called from SQL queries)

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL
- Returns NULL when the relation has no partition constraint (e.g., for non-partitioned tables or the parent table itself)
- Uses pretty-printing with indentation for readable output formatting
- The returned constraint expression represents the logical condition that determines row membership in the partition
- Commonly used in administrative queries and system information functions to inspect partition definitions
- Part of PostgreSQL's rule utilities system for reconstructing DDL from system catalogs