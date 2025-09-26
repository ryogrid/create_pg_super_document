# LockRelId

## Location
src/include/utils/rel.h: 38 - 42

## Overview
LockRelId is a structure that identifies a specific relation (table/index) within a database for locking purposes, containing both relation and database identifiers.

## Definition

```c
typedef struct LockRelId
{
	Oid			relId;			/* a relation identifier */
	Oid			dbId;			/* a database identifier */
} LockRelId;
```
## Detailed Description
LockRelId is a fundamental structure used in PostgreSQL's lock manager system to uniquely identify relations across the entire database cluster. The structure combines a relation OID with a database OID to create a globally unique identifier. This is essential because relation OIDs are only unique within a single database, but the lock manager needs to handle locks across multiple databases simultaneously.

The structure is declared in  but logically belongs to the lock manager (). It's placed in  for convenience so that it can be embedded in the  structure as a  field.

## Parameters / Member Variables
- : The Object Identifier (OID) of the relation being locked. This identifies a specific table, index, or other relation within a database.
- : The Object Identifier (OID) of the database containing the relation. This ensures the relation can be uniquely identified across the entire PostgreSQL cluster.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (typedef)
- Called from (representative examples):
  - relation_close
  - index_close
  - index_drop
  - DefineIndex
  - LockRelationId
  - UnlockRelationId
  - LockRelationIdForSession
  - UnlockRelationIdForSession

## Notes and Other Information
- This structure is fundamental to PostgreSQL's locking system and is used whenever a relation-level lock needs to be acquired or released
- The combination of  and  ensures global uniqueness across all databases in a PostgreSQL cluster
- Despite belonging conceptually to the lock manager, it's defined in  for practical reasons related to the  structure
- Used extensively in vacuum operations, index operations, and general relation access control