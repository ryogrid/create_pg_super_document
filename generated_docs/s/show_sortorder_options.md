# show_sortorder_options

## Location
src/backend/commands/explain.c: 2821 - 2878

## Overview
A static function that appends non-default sorting characteristics (collation, direction, null ordering) to a string buffer for display in PostgreSQL's EXPLAIN output.

## Definition
```c
static void
show_sortorder_options(StringInfo buf, Node *sortexpr,
                       Oid sortOperator, Oid collation, bool nullsFirst)
```

## Detailed Description
The `show_sortorder_options` function is responsible for formatting and displaying the non-default characteristics of sort ordering for a column in PostgreSQL's EXPLAIN output. It analyzes the sort expression, operator, collation, and null ordering to determine what additional information needs to be displayed beyond the basic column expression.

The function intelligently determines when to show collation information (COLLATE clause), sort direction (DESC vs ASC), custom sort operators (USING clause), and null ordering (NULLS FIRST/LAST) by comparing against the default behaviors for the data type. It only displays information that differs from the defaults to keep the output concise while being informative.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the sort order options to
- `sortexpr`: Node representing the expression being sorted
- `sortOperator`: OID of the sort operator being used
- `collation`: OID of the collation being used (InvalidOid if default)
- `nullsFirst`: Boolean indicating whether nulls should be ordered first

## Dependencies
- Functions called/Symbols referenced:
  - exprType (determines the data type of the sort expression)
  - lookup_type_cache (gets type information including default operators)
  - get_typcollation (gets the default collation for a type)
  - get_collation_name (converts collation OID to name)
  - quote_identifier (properly quotes collation names)
  - get_opname (converts operator OID to name)
  - get_equality_op_for_ordering_op (determines if operator is ascending/descending)
  - appendStringInfo/appendStringInfoString (builds output string)
- Constants referenced:
  - TYPECACHE_LT_OPR, TYPECACHE_GT_OPR (type cache flags)
- Types referenced:
  - StringInfo, Node, Oid, TypeCacheEntry
- Called from (representative examples):
  - show_sort_group_keys (when displaying sort keys with ordering information)

## Notes and Other Information
- Only displays collation if it differs from the type's default collation
- Shows DESC only for descending sorts (ASC is the default and not shown)
- Shows USING clause for non-standard sort operators that aren't the default < or > operators
- NULLS FIRST/LAST is only shown when it differs from the default behavior (NULLS LAST for ASC, NULLS FIRST for DESC)
- Includes comprehensive error handling for cache lookup failures
- The function determines the 'reverse' flag to properly handle null ordering defaults
- Uses proper SQL identifier quoting for collation names to handle special characters