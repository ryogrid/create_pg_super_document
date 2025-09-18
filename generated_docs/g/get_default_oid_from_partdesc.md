# get_default_oid_from_partdesc

## Location
src/backend/partitioning/partdesc.c: 501 - 508

## Overview
Returns the OID of the default partition from a partition descriptor, if one exists.

## Definition
```c
Oid get_default_oid_from_partdesc(PartitionDesc partdesc)
```

## Detailed Description
This function extracts the OID of the default partition from a given partition descriptor. It performs safety checks to ensure the partition descriptor and its bound information are valid, then verifies that a default partition exists using `partition_bound_has_default()`. If all conditions are met, it returns the OID from the partition descriptor's OID array at the default index position. If no default partition exists or the descriptor is invalid, it returns `InvalidOid`.

The function is a utility for the PostgreSQL partitioning system, providing a safe way to retrieve the default partition OID without direct access to the internal partition descriptor structure.

## Parameters / Member Variables
- `partdesc`: A PartitionDesc structure containing partition metadata including bound information and partition OIDs. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionDesc](../P/PartitionDesc.md) (type)
  - partition_bound_has_default
- Called from (representative examples):
  - [StorePartitionBound](../S/StorePartitionBound.md) (src/backend/catalog/heap.c:3604)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:1090)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md) (src/backend/commands/tablecmds.c:18512)
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md) (src/backend/commands/tablecmds.c:19153)

## Notes and Other Information
- Located in src/backend/partitioning/partdesc.c:501-508
- Returns InvalidOid when no default partition exists or when given invalid input
- Performs null-safety checks on both partdesc and partdesc->boundinfo
- Used primarily during partition management operations like creation, attachment, and detachment of partitions
- The function assumes that if partition_bound_has_default() returns true, the default_index is valid and within bounds of the oids array