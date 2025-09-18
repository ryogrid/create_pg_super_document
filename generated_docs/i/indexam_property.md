# indexam_property

## Location
[src/backend/utils/adt/amutils.c:151-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L151-L408)

## Overview
Core function that tests properties of index access methods, indexes, or individual index columns, providing a unified interface for property queries across different levels of the indexing system.

## Definition
```c
static Datum indexam_property(FunctionCallInfo fcinfo, const char *propname, Oid amoid, Oid index_oid, int attno)
```

## Detailed Description
This comprehensive function serves as the central dispatcher for testing various properties of PostgreSQL's index access methods. It operates at three distinct levels: access method-wide properties (when only amoid is provided), index-wide properties (when index_oid is provided with attno=0), and column-specific properties (when both index_oid and attno>0 are provided). The function first converts the property name to an enum using lookup_prop_name, then delegates to AM-specific property routines if available, and finally provides generic handling for standard properties. It handles properties ranging from ordering capabilities (ASC/DESC, NULLS positioning) to scan types (index scan, bitmap scan) to structural features (multi-column, uniqueness constraints).

## Parameters / Member Variables
- `fcinfo`: Function call information context for PostgreSQL function calls
- `propname`: String name of the property to test (e.g., "asc", "can_unique", "orderable")
- `amoid`: OID of the access method (mutually exclusive with index_oid for AM-level queries)
- `index_oid`: OID of the specific index (InvalidOid for AM-level properties)
- `attno`: Attribute number for column-specific properties (0 for index-wide properties)

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_prop_name](../l/lookup_prop_name.md) (convert property name to enum)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (system cache operations)
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md) (retrieve AM routine structure)
  - [test_indoption](../t/test_indoption.md) (test column-level indoption bits)
  - [index_open](index_open.md), index_close, index_can_return (index access for returnable property)
- Called from (representative examples):
  - [pg_indexam_has_property](../p/pg_indexam_has_property.md)
  - [pg_index_has_property](../p/pg_index_has_property.md)
  - [pg_index_column_has_property](../p/pg_index_column_has_property.md)

## Notes and Other Information
- The function is static and serves as common implementation for multiple SQL-visible functions
- Supports three operational modes: AM-level, index-level, and column-level property testing
- Handles both key and non-key columns differently (non-key columns have limited properties)
- Provides fallback generic logic when AM-specific property routines are unavailable
- Returns NULL for unknown/inapplicable properties rather than throwing errors
- Column-level properties like ASC/DESC require orderable access methods
- Distance-orderable property testing is primarily delegated to AM-specific routines
- Returnable property testing may require opening the index relation for detailed analysis