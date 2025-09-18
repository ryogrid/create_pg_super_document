# get_relation_idx_constraint_oid

## Location
src/backend/catalog/pg_constraint.c: 1043 - 1089

## Overview
Returns the OID of the constraint that is enforced by a given index in a specified relation.

## Definition
```c
Oid get_relation_idx_constraint_oid(Oid relationId, Oid indexId)
```

## Detailed Description
This function searches the pg_constraint system catalog to find the constraint that "owns" or is enforced by the specified index. It only considers constraints of types that are backed by indexes: unique constraints, primary key constraints, and exclusion constraints. Other constraint types (like check constraints or foreign key constraints) are ignored since they don't directly correspond to indexes.

The function scans through all constraints on the given relation and checks if any constraint's conindid field matches the provided index OID. When a match is found, it returns the OID of that constraint.

## Parameters / Member Variables
- `relationId`: OID of the relation to search for constraints
- `indexId`: OID of the index for which to find the associated constraint

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - CONSTRAINT_PRIMARY
  - CONSTRAINT_UNIQUE
  - CONSTRAINT_EXCLUSION
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)
  - [ConstraintCategory](../C/ConstraintCategory.md)

## Notes and Other Information
- Returns InvalidOid if no constraint is found that owns the specified index
- Only considers constraints that can be enforced by indexes (primary key, unique, exclusion)
- Uses AccessShareLock on pg_constraint for consistent reads
- The function is designed to find the single constraint that owns an index, as each constraint-backed index corresponds to exactly one constraint
- Complementary to get_constraint_index which performs the reverse lookup (finding an index for a given constraint)