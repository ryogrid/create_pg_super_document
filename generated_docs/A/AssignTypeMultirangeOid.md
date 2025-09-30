# AssignTypeMultirangeOid

## Location
[src/backend/commands/typecmds.c:2443-2475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2443-L2475)

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
  - `[table_open](../t/table_open.md)` (system catalog access)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (OID generation function)
  - `[table_close](../t/table_close.md)` (system catalog cleanup)
  
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md) (src/backend/commands/typecmds.c:1524)

## Notes and Other Information
- This function is specifically for range types introduced in PostgreSQL 14's multirange feature
- During binary upgrades, the function validates that the required multirange OID has been properly set, throwing an error if not
- The function uses AccessShareLock when accessing the pg_type catalog to prevent conflicts
- After using the binary upgrade OID, it resets `binary_upgrade_next_mrng_pg_type_oid` to InvalidOid to prevent reuse
- The allocated OID is used later in the range type creation process to establish the relationship between a range type and its multirange type
- Multirange types allow storing multiple non-overlapping ranges as a single value, extending PostgreSQL's range functionality

## Simplified Source

```c
Oid
AssignTypeMultirangeOid(void)
{
    Oid type_multirange_oid;

    // Binary upgrade mode: use pre-determined OID
    if (IsBinaryUpgrade) {
        if (!OidIsValid(binary_upgrade_next_mrng_pg_type_oid))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("pg_type multirange OID value not set when in binary upgrade mode")));

        type_multirange_oid = binary_upgrade_next_mrng_pg_type_oid;
        binary_upgrade_next_mrng_pg_type_oid = InvalidOid;
    } else {
        // Normal mode: generate new unique OID
        Relation pg_type = table_open(TypeRelationId, AccessShareLock);
        type_multirange_oid = GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid);
        table_close(pg_type, AccessShareLock);
    }

    return type_multirange_oid;
}
```