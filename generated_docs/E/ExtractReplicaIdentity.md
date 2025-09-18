# ExtractReplicaIdentity

## Location
[src/backend/access/heap/heapam.c:9119-9210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9119-L9210)

## Overview
Builds a heap tuple representing the configured REPLICA IDENTITY for use in UPDATE or DELETE operations in logical replication contexts.

## Definition


## Detailed Description
The  function constructs a tuple containing the replica identity information for a given tuple, which is essential for logical replication to identify the specific row being modified. The function handles different replica identity types (NOTHING, FULL, DEFAULT/INDEX) and optimizes by returning NULL when no identity logging is needed.

For REPLICA_IDENTITY_FULL, it returns the entire tuple, flattening any external/toasted columns. For index-based replica identity, it constructs a new tuple containing only the key columns defined by the replica identity index, setting non-key columns to NULL. The function ensures that toasted columns in the identity are always inlined for proper replication.

## Parameters / Member Variables
- : The relation containing the tuple
- : The source tuple to extract replica identity from
- : Whether replica identity columns changed or have external data (DELETE always passes true)
- : Output parameter set to true if the returned tuple is a copy rather than the original

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr
  - RelationIsLogicallyLogged
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)
  - HeapTupleHasExternal
  - [toast_flatten_tuple](../t/toast_flatten_tuple.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - bms_is_empty
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_free](../b/bms_free.md)
  - REPLICA_IDENTITY_NOTHING
  - REPLICA_IDENTITY_FULL
  - INDEX_ATTR_BITMAP_IDENTITY_KEY
  - MaxHeapAttributeNumber
  - FirstLowInvalidHeapAttributeNumber
- Called from:
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- The function is static and only used internally within heapam.c
- Returns NULL if no replica identity logging is needed or no suitable key is defined
- For REPLICA_IDENTITY_FULL, external columns are always flattened to ensure complete data availability
- For index-based replica identity, only the key columns are preserved while others are set to NULL
- The function asserts that replica identity columns are never NULL, as this would break logical replication
- Handles toasted columns in replica identity keys by forcing them to be inlined
- The  parameter helps callers understand whether they need to free the returned tuple
- Essential for logical decoding and replication to identify specific rows across different PostgreSQL instances
- The key_required optimization allows skipping identity extraction when no key columns have changed
- For relations without logical logging enabled, always returns NULL