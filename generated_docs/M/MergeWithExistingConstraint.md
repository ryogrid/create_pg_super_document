# MergeWithExistingConstraint

## Location
src/backend/catalog/heap.c: 2557 - 2711

## Overview
MergeWithExistingConstraint checks for pre-existing check constraints with the same name and either merges them with appropriate inheritance settings or reports conflicts as needed.

## Definition


## Detailed Description
MergeWithExistingConstraint is a static function that handles constraint merging during constraint addition operations. The function searches for existing constraints with the same name and relation, validates that they are identical check constraints, and either merges them by updating inheritance metadata or reports appropriate conflicts.

The function performs comprehensive conflict detection and resolution:
1. Searches pg_constraint for existing constraints with the same name and relation
2. Validates that any found constraint is a check constraint with identical expression
3. Handles special cases for partition relations and inheritance scenarios
4. Updates constraint inheritance counters and local status when merging is allowed
5. Reports various types of conflicts (duplicate names, inheritance mismatches, validation conflicts)

The merging logic handles complex inheritance scenarios, including special handling for partitioned tables where inheritance constraints have different semantics.

## Parameters / Member Variables
- : The relation for which constraint merging is being attempted
- : The name of the constraint to check for conflicts
- : The constraint expression to compare against existing constraints
- : Whether merging with existing constraints is permitted
- : Whether the new constraint is being defined locally
- : Whether the new constraint is initially valid
- : Whether the new constraint should not be inherited

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - CONSTRAINT_CHECK
  - [fastgetattr](../f/fastgetattr.md)
  - [equal](../e/equal.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - ERRCODE_DUPLICATE_OBJECT
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md)

## Notes and Other Information
- Returns true if constraint was merged (is a duplicate), false if it has a unique name, or throws error on conflicts
- Special handling for partitioned tables where inherited constraints are always non-local
- Prevents changing inherited constraints to "no inherit" status to maintain inheritance propagation
- Cannot merge constraints where child is "no inherit" or has validation mismatches
- Updates inheritance count (coninhcount) and local status (conislocal) when merging
- Issues NOTICE message when successfully merging constraints
- Validates against various constraint property conflicts including inheritance and validation status
- Related to MergeConstraintsIntoExisting function (mentioned in comments)