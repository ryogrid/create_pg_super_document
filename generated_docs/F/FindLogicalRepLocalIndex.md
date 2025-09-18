# FindLogicalRepLocalIndex

## Location
src/backend/replication/logical/relation.c: 868 - 908

## Overview
Determines the appropriate index OID to use for logical replication on the subscriber side, considering replica identity settings and available indexes.

## Definition
```c
static Oid FindLogicalRepLocalIndex(Relation localrel, LogicalRepRelation *remoterel, AttrMap *attrMap)
```

## Detailed Description
This function implements a sophisticated index selection strategy for logical replication subscribers. It follows a multi-step approach to find the most suitable index:

1. **Partitioned Table Check**: Returns InvalidOid immediately for partitioned tables, as they rely on leaf partition indexes
2. **Primary/Replica Identity Priority**: First attempts to use the relation's primary key or replica identity index via GetRelationIdentityOrPK()
3. **Full Replica Identity Fallback**: For relations with REPLICA_IDENTITY_FULL, attempts to find any usable index through FindUsableIndexForReplicaIdentityFull()
4. **No Index Available**: Returns InvalidOid if no suitable index can be found

The function is critical for logical replication performance, as having an appropriate index significantly speeds up tuple lookups during replication operations.

## Parameters / Member Variables
- `localrel`: The local relation (subscriber-side table) for which to find an index
- `remoterel`: Logical replication relation metadata from the publisher
- `attrMap`: Attribute mapping between publisher and subscriber relations

## Dependencies
- Functions called/Symbols referenced:
  - GetRelationIdentityOrPK
  - FindUsableIndexForReplicaIdentityFull
  - LogicalRepRelation (struct type)
  - AttrMap (struct type)
  - REPLICA_IDENTITY_FULL (constant)
- Called from (representative examples):
  - logicalrep_rel_open
  - logicalrep_partition_open

## Notes and Other Information
- Located in src/backend/replication/logical/relation.c:868-908
- Declared as static, indicating internal use within the relation.c module
- The function prioritizes existing primary keys and replica identity indexes over fallback strategies
- For REPLICA_IDENTITY_FULL relations, it assumes all columns are available for index scanning
- Avoids using the full planner for performance reasons, implementing a simpler index selection algorithm
- Critical for logical replication performance optimization on subscriber nodes