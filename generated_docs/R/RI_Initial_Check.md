# RI_Initial_Check

## Location
src/backend/utils/adt/ri_triggers.c: 1359 - 1653

## Overview
Validates an entire table for foreign key constraint violations using a single query during ALTER TABLE ADD FOREIGN KEY operations.

## Definition
```c
bool RI_Initial_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel)
```

## Detailed Description
This function performs a comprehensive foreign key constraint validation for an entire table using a single SQL query. Unlike trigger-based validation, it's specifically designed for ALTER TABLE ADD FOREIGN KEY operations to validate existing data before the constraint is established.

The function constructs and executes a complex LEFT OUTER JOIN query that:
1. **Permission Checking**: Verifies the current user has SELECT permissions on both tables
2. **RLS Handling**: Checks row-level security constraints and ownership
3. **Query Construction**: Builds a query to find FK rows that don't match any PK row
4. **Match Type Logic**: Handles different NULL behaviors (MATCH SIMPLE vs MATCH FULL)
5. **Performance Optimization**: Temporarily increases work_mem for efficient execution
6. **Violation Reporting**: Reports detailed constraint violation information if found

The generated query structure is:
```sql
SELECT fk.keycols FROM [ONLY] fk_table fk
LEFT OUTER JOIN [ONLY] pk_table pk ON (pk.key = fk.key)
WHERE pk.key IS NULL AND (fk.key IS NOT NULL [AND/OR ...])
```

## Parameters / Member Variables
- `trigger`: The foreign key trigger containing constraint information
- `fk_rel`: The foreign key table relation being validated
- `pk_rel`: The primary key table relation being referenced

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [quoteOneName](../q/quoteOneName.md)
  - [quoteRelationName](../q/quoteRelationName.md)
  - RIAttName, RIAttType, RIAttCollation
  - [ri_GenerateQual](../r/ri_GenerateQual.md), ri_GenerateQualCollation
  - SPI_connect, SPI_prepare, SPI_execute_snapshot, SPI_finish
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md), ExecDropSingleTupleTableSlot
  - [ri_ReportViolation](../r/ri_ReportViolation.md)
  - [ri_NullCheck](../r/ri_NullCheck.md)
- Called from (representative examples):
  - [validateForeignKeyConstraint](../v/validateForeignKeyConstraint.md)

## Notes and Other Information
- This is NOT a trigger function but a utility for constraint validation during DDL
- Returns false if permission checks fail, allowing caller to fall back to trigger method
- Temporarily adjusts work_mem and hash_mem_multiplier for performance optimization
- Uses SPI (Server Programming Interface) to execute the validation query
- Handles both partitioned and regular tables appropriately
- Located in src/backend/utils/adt/ri_triggers.c:1359-1653
- Implements sophisticated NULL handling logic based on foreign key match types
- Forces current snapshot usage to ensure data consistency during validation