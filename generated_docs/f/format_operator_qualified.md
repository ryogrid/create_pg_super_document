# format_operator_qualified

## Location
[src/backend/utils/adt/regproc.c:799-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L799-L805)

## Overview
Converts an operator OID to its fully schema-qualified textual representation, always including the schema name regardless of search_path visibility.

## Definition

```c
char *
format_operator_qualified(Oid operator_oid)
```
## Detailed Description
The  function is a specialized wrapper around  that always produces fully qualified operator names. It calls the extended function with the  flag, ensuring that the schema name is always included in the output regardless of whether the operator would be found in the current search_path.

This function is particularly useful in contexts where unambiguous operator identification is required, such as in system catalogs, dumps, or when generating SQL that needs to be portable across different database configurations with varying search_path settings.

## Parameters / Member Variables
- `operator_oid`: The OID of the operator to format
## Dependencies
- Functions called/Symbols referenced:
  - [format_operator_extended](format_operator_extended.md)
  - FORMAT_OPERATOR_FORCE_QUALIFY (flag constant)
- Called from (representative examples):
  - Referenced in header file definitions (src/include/utils/regproc.h:35)

## Notes and Other Information
- This function guarantees schema-qualified output in the format "schema.opr_name(lefttype,righttype)"
- Useful for generating portable SQL statements and in system administration contexts
- Returns a palloc'd string that must be freed by the caller
- Always includes schema qualification, making output more verbose but unambiguous
- Particularly important for operators that might exist in multiple schemas
- Located in src/backend/utils/adt/regproc.c:799-805

## Simplified Source

```c
char *format_operator_qualified(Oid operator_oid) {
    // Always format with schema qualification, regardless of search_path
    return format_operator_extended(operator_oid, FORMAT_OPERATOR_FORCE_QUALIFY);
}
```