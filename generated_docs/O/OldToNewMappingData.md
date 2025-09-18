# OldToNewMappingData

## Location
src/backend/access/heap/rewriteheap.c: 183 - 184

## Overview
OldToNewMappingData is a structure used during heap rewriting operations to maintain mappings between tuple locations in the old heap and their corresponding locations in the new heap.

## Definition


## Detailed Description
OldToNewMappingData is an essential component of PostgreSQL's heap rewriting facility that works in conjunction with UnresolvedTupData to maintain tuple reference integrity during table rewrites. This structure serves as a mapping record that tracks where tuples have been relocated from their original positions in the old heap to their new positions in the rewritten heap.

When a table undergoes a rewrite operation (such as during ALTER TABLE), every tuple must be physically moved to a new heap file. However, tuples often contain ctid (current tuple identifier) references to other tuples, forming update chains that represent different versions of the same logical row. To maintain these critical references, the system must keep track of where each tuple has been moved.

OldToNewMappingData structures are stored in hash tables and are used to resolve ctid references when processing UnresolvedTupData entries. When a tuple A references tuple B via ctid, but B's new location is needed to update A's reference, this mapping structure provides the translation from B's old location to its new location in the rewritten heap.

## Parameters / Member Variables
- : A TidHashKey structure containing the actual xmin (transaction ID) and original location of a tuple in the old heap that serves as the lookup key
- : An ItemPointerData structure storing the new location where the tuple has been placed in the rewritten heap

## Dependencies
- Functions called/Symbols referenced:
  - TidHashKey (hash key structure for tuple identification)
  - ItemPointerData (tuple identifier structure)
- Called from (representative examples):
  - begin_heap_rewrite (where old-to-new mapping tracking is initialized)

## Notes and Other Information
- This structure is the complement to UnresolvedTupData in the heap rewriting system
- Essential for maintaining MVCC (Multi-Version Concurrency Control) integrity during table restructuring
- Used exclusively during heap rewrite operations, not in normal tuple processing
- Stored in hash tables for efficient lookup during the resolution of tuple references
- Critical for ensuring that update chains remain intact after physical tuple relocation
- The mapping is bidirectional in nature - it allows translation from old locations to new locations