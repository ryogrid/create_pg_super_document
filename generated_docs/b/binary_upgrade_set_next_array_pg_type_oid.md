# binary_upgrade_set_next_array_pg_type_oid

## Location
[src/backend/utils/adt/pg_upgrade_support.c:65-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L65-L75)

## Overview
Sets the OID to be assigned to the next array type created during binary upgrade operations.

## Definition

```c
Datum
binary_upgrade_set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of PostgreSQL's binary upgrade support system that allows pg_upgrade to control OID assignment during database upgrades to preserve object identities. The function accepts an array type OID as input and stores it in the global variable , which will be used by the system when creating the next array type.

When PostgreSQL creates a new data type, it automatically creates a corresponding array type. During binary upgrades, it's crucial to preserve the OIDs of both the base type and its associated array type to maintain referential consistency. This function specifically controls the OID assignment for array types.

The function can only be called when the server is running in binary upgrade mode ( is true). This restriction ensures that OID manipulation is only allowed during controlled upgrade operations.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID value to assign to the next array type that will be created

## Dependencies
- Functions called/Symbols referenced:
  -  (macro that validates binary upgrade mode)
  -  (PostgreSQL function return macro)
- Global variable modified:
  -  (declared in )
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a SQL-callable function for use by pg_upgrade tools
- The function performs a security check via  macro which throws an error if not in binary upgrade mode
- The global variable is declared as in, making it accessible across PostgreSQL modules
- Located in
- Essential for maintaining consistency between base types and their array types during upgrades
- Works in conjunction with array type creation functions that check this variable to assign the specified OID
- PostgreSQL automatically creates array types for most base types, making this function critical for upgrade scenarios involving custom types

## Simplified Source

```c
Datum binary_upgrade_set_next_array_pg_type_oid(PG_FUNCTION_ARGS) {
    // Extract the array type OID argument
    Oid typoid = PG_GETARG_OID(0);

    // Verify we're in binary upgrade mode (throws error if not)
    CHECK_IS_BINARY_UPGRADE;

    // Store the OID for the next array type creation
    binary_upgrade_next_array_pg_type_oid = typoid;

    PG_RETURN_VOID();
}
```