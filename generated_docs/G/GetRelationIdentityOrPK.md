# GetRelationIdentityOrPK

## Location
src/backend/replication/logical/relation.c: 851 - 867

## Overview
Returns the OID of either the replica identity index or primary key index for a relation, prioritizing the replica identity index if defined.

## Definition


## Detailed Description
This function provides a unified way to obtain the most appropriate index OID for logical replication purposes. It follows a specific priority order:
1. First attempts to get the replica identity index using RelationGetReplicaIndex()
2. If no replica identity index exists (InvalidOid), falls back to the primary key index using RelationGetPrimaryKeyIndex()
3. Returns InvalidOid if neither a replica identity index nor primary key exists

The function is essential for logical replication as it determines which index should be used to uniquely identify rows during replication operations.

## Parameters / Member Variables
- : The relation (table) for which to find the identity or primary key index

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetReplicaIndex
  - RelationGetPrimaryKeyIndex
- Called from (representative examples):
  - RelationFindReplTupleByIndex
  - FindLogicalRepLocalIndex
  - check_relation_updatable
  - FindReplTupleInLocalRel

## Notes and Other Information
- Located in src/backend/replication/logical/relation.c:851-867
- Returns InvalidOid when neither replica identity nor primary key index is available
- The function prioritizes replica identity over primary key, which is important for logical replication scenarios where a custom replica identity may have been explicitly set
- Used extensively in logical replication worker processes and replication tuple handling