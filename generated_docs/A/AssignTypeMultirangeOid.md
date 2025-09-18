# AssignTypeMultirangeOid

## Location
src/backend/commands/typecmds.c: 2443 - 2475

## Overview
AssignTypeMultirangeOid is a function that pre-assigns an OID for the multirange type associated with a PostgreSQL range type, ensuring proper setup during range type creation.

## Definition
```c
Oid AssignTypeMultirangeOid(void)
```

## Detailed Description
This function is responsible for allocating a unique OID that will be used for the multirange type corresponding to a range type being created. Multirange types were introduced in PostgreSQL 14 to represent sets of non-overlapping ranges. The function handles two distinct scenarios:

1. **Binary Upgrade Mode**: When PostgreSQL is in binary upgrade mode (during pg_upgrade operations), it uses a pre-determined OID stored in `binary_upgrade_next_mrng_pg_type_oid` to maintain consistency with the original database schema.

2. **Normal Operation**: During regular range type creation, it generates a new unique OID by calling `GetNewOidWithIndex` on the pg_type system catalog.

The function ensures that every range type has a properly assigned multirange type OID before the type definition is completed, which is essential for PostgreSQL's range type system where every range type automatically gets an associated multirange type.

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**: `Oid` - A unique object identifier for the multirange type

## Dependencies
- Functions called/Symbols referenced:
  - `IsBinaryUpgrade` (macro/variable check)
  - `OidIsValid` (macro for OID validation)
  - `ereport` (error reporting function)
  - `table_open` (system catalog access)
  - `GetNewOidWithIndex` (OID generation function)
  - `table_close` (system catalog cleanup)
  
- Called from (representative examples):
  - `DefineRange` (src/backend/commands/typecmds.c:1524)

## Notes and Other Information
- This function is specifically for range types introduced in PostgreSQL 14's multirange feature
- During binary upgrades, the function validates that the required multirange OID has been properly set, throwing an error if not
- The function uses AccessShareLock when accessing the pg_type catalog to prevent conflicts
- After using the binary upgrade OID, it resets `binary_upgrade_next_mrng_pg_type_oid` to InvalidOid to prevent reuse
- The allocated OID is used later in the range type creation process to establish the relationship between a range type and its multirange type
- Multirange types allow storing multiple non-overlapping ranges as a single value, extending PostgreSQL's range functionality