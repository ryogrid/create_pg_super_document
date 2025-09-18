# regoperout

## Location
src/backend/utils/adt/regproc.c: 545 - 612

## Overview
Converts an operator OID to its string representation, including proper namespace qualification when necessary.

## Definition
```c
Datum regoperout(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regoperout` function is a PostgreSQL output function that converts an operator OID (Object Identifier) back to its string representation. This function is the inverse of `regoperin` and is used internally by PostgreSQL when displaying regoper values to users.

The function performs several sophisticated operations:
1. Handles invalid OIDs by returning "0"
2. Looks up the operator in the system catalog (`pg_operator`)
3. In bootstrap mode, returns just the operator name
4. In normal mode, determines whether namespace qualification is needed by checking if the operator name would be unique without qualification
5. If qualification is needed, prepends the schema name
6. For non-existent operators, returns the OID as a numeric string

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: OID of the operator to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_OID`: Extract OID argument from function call
  - `[pstrdup](../p/pstrdup.md)`: Duplicate a C string with palloc
  - `[SearchSysCache1](../S/SearchSysCache1.md)`: Search system cache for operator tuple
  - `HeapTupleIsValid`: Check if heap tuple is valid
  - `Form_pg_operator`: Cast to pg_operator structure
  - `GETSTRUCT`: Extract structure from heap tuple
  - `NameStr`: Extract name from Name structure
  - `IsBootstrapProcessingMode`: Check if in bootstrap mode
  - `[OpernameGetCandidates](../O/OpernameGetCandidates.md)`: Find operator candidates by name
  - `list_make1`: Create single-element list
  - `[makeString](../m/makeString.md)`: Create String node
  - `[get_namespace_name](../g/get_namespace_name.md)`: Get namespace name by OID
  - `[quote_identifier](../q/quote_identifier.md)`: Quote SQL identifier if needed
  - `[ReleaseSysCache](../R/ReleaseSysCache.md)`: Release system cache tuple
  - `[palloc](../p/palloc.md)`: PostgreSQL memory allocator
  - `sprintf`: Format string
  - `snprintf`: Safe string formatting
  - `PG_RETURN_CSTRING`: Return C string from function

- Called from (representative examples):
  - No direct references found (typically called via PostgreSQL's type system)

## Notes and Other Information
- This is an output function for the regoper data type
- The function intelligently handles namespace qualification to ensure the output can be parsed back unambiguously
- In bootstrap mode, namespace resolution is skipped for simplicity
- Returns numeric representation for operators that no longer exist in the catalog
- Part of PostgreSQL's regtype family for displaying object references in a user-friendly format