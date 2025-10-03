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
- `*stmt`: ALTER OPERATOR FAMILY statement containing context information
- `amoid`: OID of the access method
- `opfamilyoid`: OID of the operator family being modified
- `maxOpNumber`: Maximum allowed operator strategy number for this access method
- `maxProcNumber`: Maximum allowed support function number for this access method
- `optsProcNumber`: Special function number for options processing function
- `*items`: List of CreateOpClassItem objects representing operators/functions to add
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

## Simplified Source

```c
static void
AlterOpFamilyAdd(AlterOpFamilyStmt *stmt, Oid amoid, Oid opfamilyoid,
                 int maxOpNumber, int maxProcNumber, int optsProcNumber,
                 List *items)
{
    IndexAmRoutine *amroutine = GetIndexAmRoutineByAmId(amoid, false);
    List *operators = NIL;      // OpFamilyMember list for operators
    List *procedures = NIL;     // OpFamilyMember list for support procs
    ListCell *l;

    // Process each item in the ADD list
    foreach(l, items)
    {
        CreateOpClassItem *item = lfirst_node(CreateOpClassItem, l);
        OpFamilyMember *member;

        switch (item->itemtype)
        {
            case OPCLASS_ITEM_OPERATOR:
                // Validate operator number range
                if (item->number <= 0 || item->number > maxOpNumber)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errmsg("invalid operator number %d, must be between 1 and %d",
                                          item->number, maxOpNumber)));

                // Require explicit operator argument types for ALTER
                if (item->name->objargs != NIL)
                    operOid = LookupOperWithArgs(item->name, false);
                else
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                   errmsg("operator argument types must be specified in ALTER OPERATOR FAMILY")));

                // Handle optional sort family for ordering operators
                if (item->order_family)
                    sortfamilyOid = get_opfamily_oid(BTREE_AM_OID, item->order_family, false);
                else
                    sortfamilyOid = InvalidOid;

                // Create operator family member
                member = (OpFamilyMember *) palloc0(sizeof(OpFamilyMember));
                member->is_func = false;
                member->object = operOid;
                member->number = item->number;
                member->sortfamily = sortfamilyOid;
                // ALTER ADD creates soft dependencies
                member->ref_is_hard = false;
                member->ref_is_family = true;
                member->refobjid = opfamilyoid;

                assignOperTypes(member, amoid, InvalidOid);
                addFamilyMember(&operators, member);
                break;

            case OPCLASS_ITEM_FUNCTION:
                // Validate function number range
                if (item->number <= 0 || item->number > maxProcNumber)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errmsg("invalid function number %d, must be between 1 and %d",
                                          item->number, maxProcNumber)));

                funcOid = LookupFuncWithArgs(OBJECT_FUNCTION, item->name, false);

                // Create function family member
                member = (OpFamilyMember *) palloc0(sizeof(OpFamilyMember));
                member->is_func = true;
                member->object = funcOid;
                member->number = item->number;
                // ALTER ADD creates soft dependencies
                member->ref_is_hard = false;
                member->ref_is_family = true;
                member->refobjid = opfamilyoid;

                // Handle optional argument type override
                if (item->class_args)
                    processTypesSpec(item->class_args, &member->lefttype, &member->righttype);

                assignProcTypes(member, amoid, InvalidOid, optsProcNumber);
                addFamilyMember(&procedures, member);
                break;

            case OPCLASS_ITEM_STORAGETYPE:
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                               errmsg("STORAGE cannot be specified in ALTER OPERATOR FAMILY")));
                break;

            default:
                elog(ERROR, "unrecognized item type: %d", item->itemtype);
                break;
        }
    }

    // Allow access method to adjust dependencies and validate
    if (amroutine->amadjustmembers)
        amroutine->amadjustmembers(opfamilyoid, InvalidOid, operators, procedures);

    // Store operators and functions in system catalogs
    storeOperators(stmt->opfamilyname, amoid, opfamilyoid, operators, true);
    storeProcedures(stmt->opfamilyname, amoid, opfamilyoid, procedures, true);

    // Notify event triggers
    EventTriggerCollectAlterOpFam(stmt, opfamilyoid, operators, procedures);
}
```