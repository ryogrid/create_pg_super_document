# logicalrep_rel_mark_updatable

## Location
[src/backend/replication/logical/relation.c:274-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L274-L326)

## Overview
Checks if replica identity matches between local and remote relations and marks the relation entry as updatable or not based on the compatibility of replica identity keys.

## Definition
```c
static void logicalrep_rel_mark_updatable(LogicalRepRelMapEntry *entry)
```

## Detailed Description
This function determines whether a logical replication relation entry can support UPDATE and DELETE operations by comparing the local replica identity with the remote relation's replica identity. It implements a policy that allows stricter replica identity on the subscriber side (fewer columns) since this will not prevent finding unique tuples, but prevents the opposite scenario which would be problematic.

The function first attempts to get the replica identity key from the local relation, falling back to the primary key if no explicit replica identity exists. It then validates that all columns in the local replica identity are present in the remote relation's replica identity. If the validation fails, it marks the entry as not updatable, allowing later functions to handle the actual error reporting.

The function also performs safety checks to ensure system columns are not used in replica identity indexes, as this would be invalid for logical replication.

## Parameters / Member Variables
- `entry`: Pointer to LogicalRepRelMapEntry structure containing the mapping between local and remote relations, including attribute mappings and updatability status

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexAttrBitmap: Retrieves bitmap of attributes in replica identity or primary key indexes
  - bms_next_member: Iterates through bitmap of index attributes
  - bms_is_member: Checks if an attribute is present in the remote relation's replica identity
  - AttrNumberIsForUserDefinedAttr: Validates that attribute numbers are user-defined (not system columns)
  - AttrNumberGetAttrOffset: Converts attribute number to array offset
  - INDEX_ATTR_BITMAP_IDENTITY_KEY: Constant for replica identity key bitmap
  - INDEX_ATTR_BITMAP_PRIMARY_KEY: Constant for primary key bitmap
  - REPLICA_IDENTITY_FULL: Constant indicating full row replica identity
- Called from (representative examples):
  - logicalrep_rel_open: Main function for opening logical replication relations
  - logicalrep_partition_open: Function for opening partitioned relations in logical replication

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Allows asymmetric replica identity configurations where the subscriber has fewer columns than the publisher
- The function sets the updatable flag but does not raise errors - error handling is deferred to check_relation_updatable()
- Critical for logical replication safety as it prevents UPDATE/DELETE operations on relations with incompatible replica identities
- System columns in replica identity indexes are explicitly prohibited and cause immediate errors
- Falls back to primary key when no explicit replica identity is defined on the local relation