# type_is_rowtype

## Location
[src/backend/utils/cache/lsyscache.c:2655-2677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2655-L2677)

## Overview
Determines whether a PostgreSQL type represents a row/composite type, including RECORD types, named composite types, and domains over composite types.

## Definition

```c
bool
type_is_rowtype(Oid typid)
```
## Detailed Description
This convenience function provides a unified way to identify types that represent structured data with multiple fields (row types). It recognizes three categories of row types:

1. **RECORD type**: The generic anonymous composite type (RECORDOID)
2. **Named composite types**: User-defined types created with CREATE TYPE ... AS (...) or table row types
3. **Domain types over composites**: Domains that are based on composite types

The function uses a multi-step approach:
- First checks for the special RECORDOID case
- Then examines the base typtype using get_typtype()
- For composite types, returns true immediately
- For domain types, recursively checks if the underlying base type is composite

This is essential for operations that need to handle structured data differently from scalar types, such as field access, tuple construction, and record manipulation.

## Parameters / Member Variables
- : OID of the type to test for row/composite nature

## Dependencies
- Functions called/Symbols referenced:
  - [get_typtype](../g/get_typtype.md) (retrieve type category)
  - [getBaseType](../g/getBaseType.md) (resolve domain to underlying type)
  - RECORDOID (constant for generic record type)
  - TYPTYPE_COMPOSITE (constant for composite type category)
  - TYPTYPE_DOMAIN (constant for domain type category)

- Called from (representative examples):
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md) (src/backend/executor/execSRF.c:137)
  - makeWholeRowVar (src/backend/nodes/makefuncs.c:193, 230)
  - [transformExprRecurse](transformExprRecurse.md) (src/backend/parser/parse_expr.c:293)
  - [can_minmax_aggs](../c/can_minmax_aggs.md) (src/backend/optimizer/plan/planagg.c:290)
  - [json_categorize_type](../j/json_categorize_type.md) (src/backend/utils/adt/jsonfuncs.c:6035)
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md) (src/pl/plperl/plperl.c:1372)
  - compile_pltcl_function (src/pl/tcl/pltcl.c:1566, 1597)

## Notes and Other Information
- Critical for distinguishing between scalar and composite data types in the PostgreSQL type system
- Used extensively in procedural languages (PL/Perl, PL/Tcl) for proper data marshaling
- Essential for JSON processing functions that need to handle nested structures
- The function handles domain unwrapping automatically, making it safer than checking typtype directly
- Part of the type classification utilities in lsyscache.c that provide high-level type categorization