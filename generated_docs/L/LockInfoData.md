# LockInfoData

## Location
[src/include/utils/rel.h:44-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rel.h#L44-L47)

## Overview
LockInfoData is a simple wrapper structure that encapsulates lock-related information for a relation, primarily containing the LockRelId used for locking operations.

## Definition

```c
typedef struct LockInfoData
{
	LockRelId	lockRelId;
} LockInfoData;
```
## Detailed Description
LockInfoData serves as a container structure for lock-related information associated with a relation. Currently, it contains only a LockRelId field, but the structure provides a framework for potentially storing additional lock-related metadata in the future. This structure is embedded within the RelationData structure to provide each relation with its own lock identification information.

The structure is part of PostgreSQL's relation management system and works in conjunction with the lock manager to provide relation-level locking capabilities. By encapsulating the LockRelId within this structure, PostgreSQL maintains a clean separation between the relation's core data and its locking metadata.

## Parameters / Member Variables
- : A LockRelId structure that uniquely identifies this relation for locking purposes, containing both the relation OID and database OID.

## Dependencies
- Functions called/Symbols referenced:
  - [LockRelId](LockRelId.md)
- Called from (representative examples):
  - LockInfo (typedef)
  - [RelationData](../R/RelationData.md) (embedded as a member)

## Notes and Other Information
- This structure is currently minimal, containing only the LockRelId, but its design allows for future expansion with additional lock-related fields
- The structure is embedded in RelationData, making lock information readily accessible for any relation
- Part of the relation management infrastructure that bridges relation metadata with the lock manager
- The typedef LockInfo is used as a pointer type to LockInfoData for convenience