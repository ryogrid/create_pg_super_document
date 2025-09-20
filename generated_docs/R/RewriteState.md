# RewriteState

## Location
[src/include/access/rewriteheap.h:22-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/rewriteheap.h#L22-L34)

## Overview
RewriteState is an opaque pointer type that encapsulates the complete state associated with a heap rewrite operation in PostgreSQL, providing a handle to the underlying RewriteStateData structure.

## Definition

```c
typedef struct RewriteStateData *RewriteState;
```
## Detailed Description
RewriteState serves as an opaque handle to the RewriteStateData structure, which maintains all the necessary state information during table rewrite operations. This design pattern allows the rewrite facility to hide implementation details from users while providing a clean interface for heap rewrite operations. The actual state data includes source and destination relations, bulk write operations, transaction IDs for visibility checks, memory contexts, and hash tables for tracking tuple mappings.

The opaque pointer design ensures that users of the rewrite facility cannot directly access or modify the internal state, providing better encapsulation and preventing misuse of the rewrite mechanism. All interactions with the rewrite state must go through the provided API functions.

## Parameters / Member Variables
As an opaque pointer type, RewriteState does not expose member variables directly. The underlying RewriteStateData structure contains:
- : Source heap relation being rewritten
- : Destination heap relation
- : Writer for the destination relation
- : Page currently being built
- : Block number where page will be written
- : Flag indicating if logical rewriting is needed
- : Oldest transaction ID for tuple visibility determination
- : Transaction ID used as freeze cutoff point
- : Transaction ID used as cutoff for logical rewrites
- : MultiXactId used as cutoff point for multixacts
- : Memory context for hash tables and tuples
- : XLog insert LSN when rewrite started
- : Hash table for unmatched A tuples
- : Hash table for unmatched B tuples
- : Hash table for logical remapping files
- : Number of in-memory mappings

## Dependencies
- Functions called/Symbols referenced:
  - [RewriteStateData](RewriteStateData.md) (underlying structure)
  - [begin_heap_rewrite](../b/begin_heap_rewrite.md) (initialization)
  - [end_heap_rewrite](../e/end_heap_rewrite.md) (cleanup)
  - [rewrite_heap_tuple](../r/rewrite_heap_tuple.md) (tuple processing)
  - [rewrite_heap_dead_tuple](../r/rewrite_heap_dead_tuple.md) (dead tuple handling)
  - MultiXactId (transaction management)

- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)
  - [reform_and_rewrite_tuple](../r/reform_and_rewrite_tuple.md)
  - [logical_begin_heap_rewrite](../l/logical_begin_heap_rewrite.md)
  - [logical_end_heap_rewrite](../l/logical_end_heap_rewrite.md)
  - [logical_rewrite_heap_tuple](../l/logical_rewrite_heap_tuple.md)

## Notes and Other Information
- The struct definition is intentionally private to rewriteheap.c to maintain encapsulation
- Users must use the provided API functions (begin_heap_rewrite, end_heap_rewrite, etc.) to manipulate the state
- This design pattern is common in PostgreSQL for complex subsystems that need to maintain significant internal state
- The opaque pointer approach allows for future changes to the internal structure without breaking API compatibility
- All rewrite operations should be properly bracketed with begin_heap_rewrite and end_heap_rewrite calls