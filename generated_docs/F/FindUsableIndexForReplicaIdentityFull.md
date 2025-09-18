# FindUsableIndexForReplicaIdentityFull

## Location
src/backend/replication/logical/relation.c: 745 - 803

## Overview
Finds the first suitable index on a local relation that can be used by the logical replication apply worker when REPLICA IDENTITY FULL is configured.

## Definition
```c
static Oid FindUsableIndexForReplicaIdentityFull(Relation localrel, AttrMap *attrmap)
```

## Detailed Description
This function searches through all indexes on a local relation to find one that is suitable for logical replication operations when the remote relation is configured with REPLICA IDENTITY FULL. It iterates through the relation's index list, opens each index, builds index information, and tests whether the index can be used for replica identity purposes using the provided attribute map.

The function is designed to support logical replication scenarios where a full row image is used for identifying rows during UPDATE and DELETE operations. It returns the OID of the first usable index found, or InvalidOid if no suitable index exists.

## Parameters / Member Variables
- `localrel`: The local relation for which to find a usable index
- `attrmap`: Attribute map that describes the mapping between local and remote relation attributes

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - foreach_oid (macro)
  - index_open
  - BuildIndexInfo
  - IsIndexUsableForReplicaIdentityFull
  - index_close
- Types referenced:
  - Relation
  - AttrMap
  - List
  - IndexInfo
  - Oid
- Constants referenced:
  - AccessShareLock
  - InvalidOid
- Called from (representative examples):
  - FindLogicalRepLocalIndex

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Designed specifically for REPLICA IDENTITY FULL scenarios in logical replication
- Uses AccessShareLock when opening indexes to prevent concurrent modifications during analysis
- Returns immediately upon finding the first suitable index rather than evaluating all indexes
- Properly closes each index after evaluation to prevent resource leaks
- The selection logic is delegated to IsIndexUsableForReplicaIdentityFull for modularity
- Part of the broader logical replication index selection mechanism for ensuring efficient row identification