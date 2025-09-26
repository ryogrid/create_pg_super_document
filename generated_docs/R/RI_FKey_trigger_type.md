# RI_FKey_trigger_type

## Location
[src/backend/utils/adt/ri_triggers.c:3001-3023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L3001-L3023)

## Overview
Determines the type of referential integrity (RI) trigger based on a trigger function OID, specifically whether it is attached to a primary key (PK) or foreign key (FK) relation.

## Definition

```c
int
RI_FKey_trigger_type(Oid tgfoid)
```
## Detailed Description
This function analyzes a given trigger function OID to classify it as one of three types of referential integrity triggers:

1. **RI_TRIGGER_PK**: Triggers that are fired on the primary key (referenced) table when changes occur that might affect foreign key constraints. These include CASCADE, RESTRICT, SET NULL, SET DEFAULT, and NO ACTION triggers for both DELETE and UPDATE operations.

2. **RI_TRIGGER_FK**: Triggers that are fired on the foreign key (referencing) table to validate foreign key constraints during INSERT and UPDATE operations.

3. **RI_TRIGGER_NONE**: Indicates that the given OID does not correspond to any recognized referential integrity trigger function.

The function uses a switch statement to categorize trigger function OIDs based on predefined constants representing different RI trigger types. This classification is essential for PostgreSQL's foreign key constraint enforcement system, allowing the system to properly handle cascade operations, constraint validation, and other referential integrity actions.

## Parameters / Member Variables
- : The OID (Object Identifier) of the trigger function to be classified

## Dependencies
- Functions called/Symbols referenced:
  - F_RI_FKEY_CASCADE_DEL (constant for cascade delete trigger)
  - F_RI_FKEY_CASCADE_UPD (constant for cascade update trigger)
  - F_RI_FKEY_RESTRICT_DEL (constant for restrict delete trigger)
  - F_RI_FKEY_RESTRICT_UPD (constant for restrict update trigger)
  - F_RI_FKEY_SETNULL_DEL (constant for set null delete trigger)
  - F_RI_FKEY_SETNULL_UPD (constant for set null update trigger)
  - F_RI_FKEY_SETDEFAULT_DEL (constant for set default delete trigger)
  - F_RI_FKEY_SETDEFAULT_UPD (constant for set default update trigger)
  - F_RI_FKEY_NOACTION_DEL (constant for no action delete trigger)
  - F_RI_FKEY_NOACTION_UPD (constant for no action update trigger)
  - F_RI_FKEY_CHECK_INS (constant for foreign key check insert trigger)
  - F_RI_FKEY_CHECK_UPD (constant for foreign key check update trigger)
  - RI_TRIGGER_PK (return value for PK-related triggers)
  - RI_TRIGGER_FK (return value for FK-related triggers)
  - RI_TRIGGER_NONE (return value for non-RI triggers)

- Called from (representative examples):
  - [GetForeignKeyActionTriggers](../G/GetForeignKeyActionTriggers.md) (src/backend/commands/tablecmds.c:11315)
  - [GetForeignKeyCheckTriggers](../G/GetForeignKeyCheckTriggers.md) (src/backend/commands/tablecmds.c:11376)
  - [AfterTriggerSaveEvent](../A/AfterTriggerSaveEvent.md) (src/backend/commands/trigger.c:6425)
  - [ExecCrossPartitionUpdateForeignKey](../E/ExecCrossPartitionUpdateForeignKey.md) (src/backend/executor/nodeModifyTable.c:2238)

## Notes and Other Information
- This function is crucial for PostgreSQL's referential integrity enforcement system
- It helps distinguish between triggers that fire on the primary key table versus the foreign key table
- The classification enables proper handling of cascade operations and constraint validation
- Located in src/backend/utils/adt/ri_triggers.c at lines 3001-3023
- Returns integer values representing different trigger types: RI_TRIGGER_PK, RI_TRIGGER_FK, or RI_TRIGGER_NONE