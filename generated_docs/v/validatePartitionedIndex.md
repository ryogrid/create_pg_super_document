# validatePartitionedIndex

## Location
src/backend/commands/tablecmds.c: 20027 - 20127

## Overview
Validates and potentially marks as valid a partitioned index by checking if all partition indexes are attached and valid, implementing a recursive validation mechanism for multi-level partition hierarchies.

## Definition


## Detailed Description
This function performs comprehensive validation of a partitioned index to determine if it should be marked as valid. The validation process involves:

1. **Inheritance Scanning**: Scans pg_inherits to find all child indexes of the partitioned index
2. **Validity Checking**: For each child index found, checks if it's marked as valid in pg_index
3. **Completeness Verification**: Compares the count of valid child indexes against the number of partitions in the partitioned table
4. **Catalog Update**: If all partitions have valid indexes, marks the parent partitioned index as valid by updating pg_index.indisvalid
5. **Recursive Validation**: If the current index is itself a partition of a larger partitioned index, recursively validates the parent hierarchy

The function ensures that a partitioned index is only considered valid when all its constituent partition indexes are present and valid. This maintains consistency in the partitioning system and ensures that queries against partitioned tables can rely on complete index coverage.

## Parameters / Member Variables
- : The partitioned index relation to validate
- : The partitioned table relation that owns the index

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - table_close
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - SearchSysCacheCopy1
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - CommandCounterIncrement
  - [get_partition_parent](../g/get_partition_parent.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
  - [validatePartitionedIndex](validatePartitionedIndex.md) (recursive call)
- Called from (representative examples):
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)
  - [validatePartitionedIndex](validatePartitionedIndex.md) (recursive self-call)

## Notes and Other Information
- This function is called after each partition index attachment to check if the parent can be marked valid
- Uses AccessShareLock for reading operations and RowExclusiveLock when updating catalog tables
- The recursive nature allows proper validation of multi-level partition hierarchies
- CommandCounterIncrement() ensures that recent catalog changes are visible during recursive validation
- The function assumes the partitioned index starts as invalid and only marks it valid when all conditions are met
- Error handling includes cache lookup failures with appropriate error messages
- The validation is transactional - either the index becomes valid or remains invalid, with no partial states