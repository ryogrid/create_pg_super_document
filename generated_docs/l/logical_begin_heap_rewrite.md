# logical_begin_heap_rewrite

## Location
src/backend/access/heap/rewriteheap.c: 759 - 806

## Overview
Initializes logical rewrite support during heap rewriting operations, setting up the necessary infrastructure to track tuple mappings for logical decoding when the table is accessible during logical replication.

## Definition


## Detailed Description
This function prepares for logging logical mappings during a heap rewrite operation when necessary for logical decoding support. It determines whether the rewritten table needs logical rewrite tracking by checking if the relation is accessible during logical decoding and if there are active logical replication slots. If logical rewrite support is needed, it initializes the necessary data structures including a hash table to track rewrite mappings organized by transaction ID.

The function implements an optimization where it only enables logical rewrite tracking if:
1. The relation is accessible in logical decoding
2. There are active logical replication slots (logical_xmin is valid)

When enabled, it sets up the RewriteState with logical rewrite parameters and creates a hash table to store rewrite mappings that will be used to maintain the correctness of (relfilelocator,ctid) => (cmin, cmax) mappings during logical decoding.

## Parameters / Member Variables
- : RewriteState structure that maintains the state of the heap rewrite operation, including logical rewrite settings and mapping structures

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsAccessibleInLogicalDecoding
  - [ProcArrayGetReplicationSlotXmin](../P/ProcArrayGetReplicationSlotXmin.md)
  - [GetXLogInsertRecPtr](../G/GetXLogInsertRecPtr.md)
  - [hash_create](../h/hash_create.md)
- Called from (representative examples):
  - [begin_heap_rewrite](../b/begin_heap_rewrite.md)

## Notes and Other Information
- This is a static function internal to the rewriteheap.c module
- The function uses lazy initialization - it only sets up logical rewrite infrastructure when actually needed
- Creates a hash table with transaction IDs as keys and RewriteMappingFile structures as values
- The logical rewrite system is crucial for maintaining consistency in logical replication during DDL operations that rewrite heap files
- Part of PostgreSQL's logical replication infrastructure that ensures catalog tuple visibility information (cmin/cmax) remains correct after heap rewrites