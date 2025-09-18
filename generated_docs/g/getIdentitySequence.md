# getIdentitySequence

## Location
src/backend/catalog/pg_depend.c: 946 - 988

## Overview
Retrieves the identity sequence associated with a specific column of a relation, with error handling for cases where exactly one sequence is not found.

## Definition
Oid getIdentitySequence(Relation rel, AttrNumber attnum, bool missing_ok)

## Detailed Description
This function finds the identity sequence owned by a specific column of a relation. It handles the complexity of partitioned tables by looking up sequences in the topmost partitioned table, since identity sequences are associated with the parent table rather than individual partitions. The function includes comprehensive error checking to ensure exactly one identity sequence is found, unless the missing_ok parameter allows for graceful handling of missing sequences.

## Parameters / Member Variables
- `rel`: The relation (table) to examine for identity sequences
- `attnum`: The attribute number (column) for which to find the identity sequence
- `missing_ok`: If true, allows the function to return InvalidOid when no sequence is found instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetForm
  - get_partition_ancestors
  - get_attname
  - llast_oid
  - get_attnum
  - list_free
  - getOwnedSequences_internal
  - linitial_oid
  - DEPENDENCY_INTERNAL
  - InvalidAttrNumber
- Called from (representative examples):
  - ATExecDropIdentity
  - transformTableLikeClause
  - transformAlterTableStmt
  - build_column_default

## Notes and Other Information
The function handles partitioned tables specially by traversing up to the topmost partitioned table, as identity sequences are stored at the partition root level. It performs attribute name resolution to handle cases where column ordering might differ between partitions. The function enforces strict validation by default, ensuring exactly one identity sequence exists unless missing_ok is true. This is crucial for maintaining data integrity in identity column operations.