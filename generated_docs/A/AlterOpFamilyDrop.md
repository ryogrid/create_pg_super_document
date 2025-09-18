# AlterOpFamilyDrop

## Location
[src/backend/commands/opclasscmds.c:1030-1107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1030-L1107)

## Overview
Implements the DROP portion of ALTER OPERATOR FAMILY commands by removing existing operators and support functions from an operator family.

## Definition


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
- : ALTER OPERATOR FAMILY statement containing context information
- : OID of the access method
- : OID of the operator family being modified
- : Maximum allowed operator strategy number for validation
- : Maximum allowed support function number for validation
- : List of CreateOpClassItem objects representing operators/functions to remove

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