# MergeConstraintsIntoExisting

## Location
src/backend/commands/tablecmds.c: 16016 - 16140

## Overview
MergeConstraintsIntoExisting is a static function that validates constraint compatibility between child and parent relations during inheritance creation, and increments the inheritance count for matching constraints.

## Definition


## Detailed Description
This function performs comprehensive constraint validation and merging when establishing inheritance relationships between tables. It ensures that all inheritable check constraints from the parent relation exist in the child relation with equivalent definitions, then updates the inheritance counts accordingly. The function uses a nested scanning approach to compare constraints by name and functional equivalence.

For each check constraint in the parent relation, the function:
1. Skips non-check constraints and NO INHERIT constraints
2. Searches for a matching constraint by name in the child relation
3. Validates that the constraints are functionally equivalent using constraints_equivalent()
4. Ensures the child constraint is not marked as NO INHERIT
5. Validates that validation status is compatible (valid parent cannot merge with invalid child)
6. Increments the constraint's inheritance count (coninhcount)
7. For partitions, sets conislocal to false since partitions cannot have local constraints
8. Updates the catalog with the modified constraint information

The function uses an O(N^2) algorithm but is considered acceptable for typical constraint counts (10-100).

## Parameters / Member Variables
- : The child relation being established as an inheritor
- : The parent relation to inherit constraints from

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - RelationGetRelid
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - systable_endscan
  - GETSTRUCT
  - NameStr
  - strcmp
  - constraints_equivalent
  - heap_copytuple
  - CatalogTupleUpdate
  - heap_freetuple
  - table_close
  - ereport
  - RelationGetRelationName
  - RelationGetDescr
  - Form_pg_constraint
  - SysScanDesc
  - CONSTRAINT_CHECK
- Called from (representative examples):
  - CreateInheritance
  - child_dependency_type

## Notes and Other Information
- Currently requires all parent check constraints to exist in child - missing constraints result in an error
- Only processes check constraints (CONSTRAINT_CHECK), ignoring other constraint types
- Ignores parent constraints marked with NO INHERIT flag
- Prevents merging if child constraint is marked NO INHERIT or has incompatible validation status
- Uses RowExclusiveLock on pg_constraint catalog for safe concurrent access
- Algorithm complexity is O(N^2) but acceptable for typical constraint counts
- For partitioned tables, ensures inherited constraints are marked as non-local (conislocal = false)
- Prevents inheritance count overflow by checking for negative values after increment
- All constraint modifications are transactional and will rollback if the operation fails later
- Future consideration mentioned for auto-creating missing constraints like CREATE TABLE