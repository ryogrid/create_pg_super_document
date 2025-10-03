# AlterOpFamilyDrop

## Location
[src/backend/commands/opclasscmds.c:1030-1107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1030-L1107)

## Overview
Implements the DROP portion of ALTER OPERATOR FAMILY commands by removing existing operators and support functions from an operator family.

## Definition

```c
static void
AlterOpFamilyDrop(AlterOpFamilyStmt *stmt, Oid amoid, Oid opfamilyoid,
				  int maxOpNumber, int maxProcNumber, List *items)
```
## Detailed Description
AlterOpFamilyDrop processes the removal of operators and support functions from an existing operator family. Unlike AlterOpFamilyAdd, this function doesn't need to lookup actual operator or function OIDs - it only needs to identify which catalog entries to remove based on strategy/support numbers and argument types.

Key characteristics:
- Processes operators (OPCLASS_ITEM_OPERATOR) and functions (OPCLASS_ITEM_FUNCTION) for removal
- Uses strategy numbers and type signatures to identify entries to remove
- Does not resolve actual operator/function OIDs since removal is based on catalog keys
- No access method callback (amadjustmembers) since this is purely a deletion operation
- Removes entries from pg_amop and pg_amproc catalogs
- STORAGE items are prohibited by grammar rules

The function creates OpFamilyMember structures that contain only the information needed to identify catalog entries for deletion (numbers and type signatures), then delegates to dropOperators and dropProcedures for the actual removal.

## Parameters / Member Variables
- `*stmt`: ALTER OPERATOR FAMILY statement containing context information
- `amoid`: OID of the access method
- `opfamilyoid`: OID of the operator family being modified
- `maxOpNumber`: Maximum allowed operator strategy number for validation
- `maxProcNumber`: Maximum allowed support function number for validation
- `*items`: List of CreateOpClassItem objects representing operators/functions to remove
## Dependencies
- Functions called/Symbols referenced:
  - [processTypesSpec](../p/processTypesSpec.md)
  - [addFamilyMember](../a/addFamilyMember.md)
  - [dropOperators](../d/dropOperators.md)
  - [dropProcedures](../d/dropProcedures.md)
  - [EventTriggerCollectAlterOpFam](../E/EventTriggerCollectAlterOpFam.md)
- Called from (representative examples):
  - [AlterOpFamily](AlterOpFamily.md) (when isDrop is true)

## Notes and Other Information
- Does not need to lookup actual operator or function OIDs, unlike AlterOpFamilyAdd
- Identification of entries to remove is based on strategy/support numbers plus type signatures
- STORAGE type specification is prevented by parser grammar rules
- The function validates number ranges but doesn't need to validate object existence
- Event triggers are notified for proper extension dependency tracking
- No amadjustmembers callback since access methods don't need to validate deletions
- [OpFamilyMember](../O/OpFamilyMember.md) structures contain only identification information, not full object details
- Error handling focuses on number validation rather than object existence checking

## Simplified Source

```c
static void
AlterOpFamilyDrop(AlterOpFamilyStmt *stmt, Oid amoid, Oid opfamilyoid,
                  int maxOpNumber, int maxProcNumber, List *items)
{
    List *operators = NIL;      // OpFamilyMember list for operators
    List *procedures = NIL;     // OpFamilyMember list for support procs
    ListCell *l;

    // Process each item in the DROP list
    foreach(l, items)
    {
        CreateOpClassItem *item = lfirst_node(CreateOpClassItem, l);
        Oid lefttype, righttype;
        OpFamilyMember *member;

        switch (item->itemtype)
        {
            case OPCLASS_ITEM_OPERATOR:
                // Validate operator number range
                if (item->number <= 0 || item->number > maxOpNumber)
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                             errmsg("invalid operator number %d, must be between 1 and %d",
                                    item->number, maxOpNumber)));

                // Extract type signature for identification
                processTypesSpec(item->class_args, &lefttype, &righttype);

                // Create operator family member for removal
                member = (OpFamilyMember *) palloc0(sizeof(OpFamilyMember));
                member->is_func = false;
                member->number = item->number;
                member->lefttype = lefttype;
                member->righttype = righttype;
                addFamilyMember(&operators, member);
                break;

            case OPCLASS_ITEM_FUNCTION:
                // Validate function number range
                if (item->number <= 0 || item->number > maxProcNumber)
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                             errmsg("invalid function number %d, must be between 1 and %d",
                                    item->number, maxProcNumber)));

                // Extract type signature for identification
                processTypesSpec(item->class_args, &lefttype, &righttype);

                // Create function family member for removal
                member = (OpFamilyMember *) palloc0(sizeof(OpFamilyMember));
                member->is_func = true;
                member->number = item->number;
                member->lefttype = lefttype;
                member->righttype = righttype;
                addFamilyMember(&procedures, member);
                break;

            case OPCLASS_ITEM_STORAGETYPE:
                // Grammar prevents this from appearing
            default:
                elog(ERROR, "unrecognized item type: %d", item->itemtype);
                break;
        }
    }

    // Remove entries from system catalogs
    dropOperators(stmt->opfamilyname, amoid, opfamilyoid, operators);
    dropProcedures(stmt->opfamilyname, amoid, opfamilyoid, procedures);

    // Notify event triggers
    EventTriggerCollectAlterOpFam(stmt, opfamilyoid, operators, procedures);
}
```