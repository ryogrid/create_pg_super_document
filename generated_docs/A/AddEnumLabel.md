# AddEnumLabel

## Location
src/backend/catalog/pg_enum.c: 292 - 606

## Overview
Adds a new label to an existing enum type, with support for positioning the label before/after existing values and handling concurrent modifications through sophisticated OID allocation and transaction tracking.

## Definition


## Detailed Description
AddEnumLabel implements the core functionality for ALTER TYPE ADD VALUE operations in PostgreSQL. This complex function handles adding new enum values to existing enum types with several sophisticated features:

1. **Concurrent Access Control**: Uses ExclusiveLock on the enum type to prevent concurrent modifications while allowing read access by other backends.

2. **Positioning Logic**: Supports adding the new value at the end (default) or before/after a specified existing value using enumsortorder calculations.

3. **OID Allocation Strategy**: Implements intelligent OID allocation that prefers even-numbered OIDs for performance (enabling direct OID comparison), but falls back to odd OIDs when necessary to maintain correct sort order.

4. **Precision Handling**: Uses volatile float4 variables and renumbering logic to handle floating-point precision issues when inserting values between existing ones.

5. **Transaction Tracking**: Maintains uncommitted_enum_values hash table to track values added in the current transaction, supporting proper enum constraint enforcement.

6. **Binary Upgrade Support**: Special handling for pg_dump binary upgrade scenarios with predetermined OIDs.

## Parameters / Member Variables
- : The OID of the enum type to add the value to
- : The string value of the new enum label to add
- : Optional existing enum label to position relative to (NULL for end placement)
- : When neighbor is specified, whether to place the new value after (true) or before (false) the neighbor
- : If true, skip with NOTICE rather than ERROR when label already exists

## Dependencies
- Functions called/Symbols referenced:
  - [LockDatabaseObject](../L/LockDatabaseObject.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - SearchSysCacheList1
  - qsort (with sort_order_cmp)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [RenumberEnumType](../R/RenumberEnumType.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [EnumTypeUncommitted](../E/EnumTypeUncommitted.md)
  - [init_uncommitted_enum_values](../i/init_uncommitted_enum_values.md)
  - [hash_search](../h/hash_search.md)
- Called from:
  - [AlterEnum](AlterEnum.md) (src/backend/commands/typecmds.c:1299)

## Notes and Other Information
- Uses enumsortorder values to maintain logical ordering separate from OID ordering
- The "restart" logic handles cases where float4 precision issues require renumbering all existing values
- Even/odd OID allocation strategy is a performance optimization for enum comparison operations
- Binary upgrade mode restricts BEFORE/AFTER positioning to maintain OID consistency
- Uncommitted enum value tracking is skipped for enum types created in the same transaction (optimization)
- The function validates label length against NAMEDATALEN before processing
- Extensive error handling for duplicate labels, invalid neighbors, and binary upgrade constraints