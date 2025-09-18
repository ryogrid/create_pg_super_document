# AssignTypeMultirangeArrayOid

## Location
src/backend/commands/typecmds.c: 2476 - 2517

## Overview
AssignTypeMultirangeArrayOid is a function that pre-assigns an OID for the array type of a multirange type, ensuring proper setup of the pg_type.typarray field during range type creation.

## Definition
```c
Oid AssignTypeMultirangeArrayOid(void)
```

## Detailed Description
This function is responsible for allocating a unique OID that will be used for the array type corresponding to a multirange type being created. In PostgreSQL's type system, every type (including multirange types) can have an associated array type. The function handles two distinct scenarios:

1. **Binary Upgrade Mode**: When PostgreSQL is in binary upgrade mode (during pg_upgrade operations), it uses a pre-determined OID stored in `binary_upgrade_next_mrng_array_pg_type_oid` to maintain consistency with the original database schema.

2. **Normal Operation**: During regular range type creation, it generates a new unique OID by calling `GetNewOidWithIndex` on the pg_type system catalog.

The function ensures that every multirange type has a properly assigned array type OID before the type definition is completed, which is essential for PostgreSQL's type system where every multirange type can have an associated array type (e.g., int4multirange[]).

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**: `Oid` - A unique object identifier for the multirange array type

## Dependencies
- Functions called/Symbols referenced:
  - `IsBinaryUpgrade` (macro/variable check)
  - `OidIsValid` (macro for OID validation)
  - `ereport` (error reporting function)
  - `table_open` (system catalog access)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (OID generation function)
  - `table_close` (system catalog cleanup)
  
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md) (src/backend/commands/typecmds.c:1525)

## Notes and Other Information
- This function is specifically for the array types of multirange types, creating a three-level type hierarchy: base type → multirange type → multirange array type
- During binary upgrades, the function validates that the required multirange array OID has been properly set, throwing an error if not
- The function uses AccessShareLock when accessing the pg_type catalog to prevent conflicts
- After using the binary upgrade OID, it resets `binary_upgrade_next_mrng_array_pg_type_oid` to InvalidOid to prevent reuse
- The allocated OID is used later in the range type creation process to establish the relationship between a multirange type and its array type
- This completes the full type family for range types: range, range[], multirange, and multirange[]
- The function is part of PostgreSQL 14's multirange feature implementation