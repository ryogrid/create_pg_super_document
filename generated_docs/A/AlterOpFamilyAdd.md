# AlterOpFamilyAdd

## Location
[src/backend/commands/opclasscmds.c:881-1029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L881-L1029)

## Overview
Implements the ADD portion of ALTER OPERATOR FAMILY commands by adding new operators and support functions to an existing operator family.

## Definition

```c
static void
AlterOpFamilyAdd(AlterOpFamilyStmt *stmt, Oid amoid, Oid opfamilyoid,
				 int maxOpNumber, int maxProcNumber, int optsProcNumber,
				 List *items)
```
## Detailed Description
AlterOpFamilyAdd processes the addition of operators and support functions to an existing operator family. It validates each item in the provided list, ensuring operator and function numbers are within valid ranges for the access method, resolves operator and function references, and creates the necessary catalog entries.

Key aspects:
- Processes operators (OPCLASS_ITEM_OPERATOR) and functions (OPCLASS_ITEM_FUNCTION)
- Requires explicit argument types for operators (unlike CREATE OPERATOR CLASS)
- Creates soft dependencies (ref_is_hard = false) historically for ALTER ADD operations
- Prohibits STORAGE type specification (only allowed in CREATE OPERATOR CLASS)
- Allows access method-specific validation through amadjustmembers callback
- Creates pg_amop and pg_amproc entries with family-level dependencies

## Parameters / Member Variables
- : ALTER OPERATOR FAMILY statement containing context information
- : OID of the access method
- : OID of the operator family being modified
- : Maximum allowed operator strategy number for this access method
- : Maximum allowed support function number for this access method  
- : Special function number for options processing function
- : List of CreateOpClassItem objects representing operators/functions to add

## Dependencies
- Functions called/Symbols referenced:
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - [LookupOperWithArgs](../L/LookupOperWithArgs.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [get_opfamily_oid](../g/get_opfamily_oid.md)
  - [assignOperTypes](../a/assignOperTypes.md)
  - [assignProcTypes](../a/assignProcTypes.md)
  - [addFamilyMember](../a/addFamilyMember.md)
  - [processTypesSpec](../p/processTypesSpec.md)
  - [storeOperators](../s/storeOperators.md)
  - [storeProcedures](../s/storeProcedures.md)
  - [EventTriggerCollectAlterOpFam](../E/EventTriggerCollectAlterOpFam.md)
- Called from (representative examples):
  - [AlterOpFamily](AlterOpFamily.md) (when isDrop is false)

## Notes and Other Information
- Unlike DefineOpClass, this function requires explicit operator argument types - no defaulting to the opclass input type
- Creates soft dependencies on the operator family rather than hard dependencies like CREATE OPERATOR CLASS
- STORAGE type specification is explicitly prohibited with a syntax error
- The access method can override dependency choices and perform additional validation via amadjustmembers
- Event triggers are notified of the changes for proper extension support
- Uses InvalidOid for opclass parameter when calling amadjustmembers since this operates at family level
- Function performs immediate validation of operator/function existence and number ranges