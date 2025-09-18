# check_new_partition_bound

## Location
src/backend/partitioning/partbounds.c: 2896 - 3250

## Overview
Validates that a new partition's bounds do not overlap with any existing partitions and enforces partition strategy-specific constraints.

## Definition
```c
void check_new_partition_bound(char *relname, Relation parent,
                             PartitionBoundSpec *spec, ParseState *pstate)
```

## Detailed Description
This comprehensive validation function ensures partition bound integrity when adding new partitions. For DEFAULT partitions, it checks for existing defaults. For HASH partitions, it enforces the modulus factor rule (each modulus must be a factor of larger moduli) and detects remainder conflicts. For LIST partitions, it searches for duplicate values including NULL handling. For RANGE partitions, it validates bound ordering, checks for empty ranges, and uses binary search to detect overlaps with existing partitions. The function provides detailed error messages with source location information for debugging.

## Parameters / Member Variables
- `relname`: Name of the new partition being created
- `parent`: Parent partitioned table relation
- `spec`: Partition bound specification containing bounds and strategy
- `pstate`: Parse state for error reporting with location information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetPartitionKey
  - RelationGetPartitionDesc
  - partition_bound_has_default
  - partition_hash_bsearch
  - partition_list_bsearch
  - partition_range_bsearch
  - make_one_partition_rbound
  - partition_rbound_cmp
  - get_rel_name
  - DatumGetInt32
  - PartitionBoundSpec
  - PartitionKey
  - PartitionDesc
  - PartitionBoundInfo
  - PartitionRangeBound
  - PartitionRangeDatum
  - PartitionRangeDatumKind
- Called from (representative examples):
  - DefineRelation
  - ATExecAttachPartition

## Notes and Other Information
- This is a public function (non-static) used by table creation and partition attachment commands
- Implements comprehensive validation for all PostgreSQL partition strategies (HASH, LIST, RANGE)
- For HASH partitions, enforces the mathematical constraint that moduli form a factorization chain
- Provides precise error location reporting for syntax and semantic errors
- Critical for maintaining partition constraint integrity and preventing data inconsistencies
- Located in src/backend/partitioning/partbounds.c:2896-3250