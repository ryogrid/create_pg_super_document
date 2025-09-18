# truncate_check_rel

## Location
src/backend/commands/tablecmds.c: 2302 - 2349

## Overview
truncate_check_rel validates that a given relation is safe and allowed to be truncated, performing relation type checks, foreign data wrapper capability validation, and system catalog protection.

## Definition
```c
static void truncate_check_rel(Oid relid, Form_pg_class reltuple)
```

## Detailed Description
This function performs essential safety checks before allowing a relation to be truncated:

1. **Relation Type Validation**: Ensures the relation is of an appropriate type for truncation:
   - Regular tables (RELKIND_RELATION) are always allowed
   - Partitioned tables (RELKIND_PARTITIONED_TABLE) are allowed for validation purposes
   - Foreign tables (RELKIND_FOREIGN_TABLE) are allowed only if their FDW supports truncation
   - Other relation types (views, indexes, sequences, etc.) are rejected

2. **Foreign Data Wrapper Support**: For foreign tables, verifies that the associated FDW has implemented the ExecForeignTruncate callback function, which is required for truncation support

3. **System Catalog Protection**: Prevents truncation of system catalogs unless:
   - allowSystemTableMods is enabled, OR
   - The operation is part of a binary upgrade and the target is pg_largeobject (special case for pg_upgrade compatibility)

4. **Hook Invocation**: Calls the object truncate hook to allow extensions to perform additional validation or logging

The function raises appropriate errors for invalid relations or insufficient permissions, ensuring that only safe truncation operations proceed.

## Parameters / Member Variables
- `relid`: Object ID of the relation to check
- `reltuple`: Form_pg_class tuple containing the relation's catalog information

## Dependencies
- Functions called/Symbols referenced:
  - [GetForeignServerIdByRelId](../G/GetForeignServerIdByRelId.md)
  - [GetFdwRoutineByServerId](../G/GetFdwRoutineByServerId.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - InvokeObjectTruncateHook
- Called from (representative examples):
  - [ExecuteTruncate](../E/ExecuteTruncate.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [RangeVarCallbackForTruncate](../R/RangeVarCallbackForTruncate.md)

## Notes and Other Information
- This function is a subroutine shared by multiple truncate-related functions to ensure consistent validation
- The pg_largeobject exception is specifically designed to support pg_upgrade operations where the relfilenode needs to be changed
- Foreign table truncation support depends entirely on the capabilities of the foreign data wrapper
- System catalog protection is a critical security feature that prevents accidental data loss in PostgreSQL's internal tables
- The function provides specific error messages to help users understand why truncation is not allowed for particular relations