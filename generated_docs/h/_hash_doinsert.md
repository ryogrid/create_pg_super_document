# _hash_doinsert

## Location
[src/backend/access/hash/hashinsert.c:38-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashinsert.c#L38-L273)

## Overview
The  function handles the insertion of a single index tuple into a hash index, including all necessary logic for bucket management, overflow pages, and potential table expansion.

## Definition


## Detailed Description
This is the core insertion function for PostgreSQL's hash index implementation. It performs a complete tuple insertion process including:

1. **Hash key computation and validation**: Extracts the hash key from the index tuple and validates that the tuple size doesn't exceed hash page limits.

2. **Bucket location and locking**: Locates the appropriate bucket page using the hash key and acquires write locks.

3. **Split completion handling**: If the target bucket is in the process of being split, completes the split operation first to potentially create space for the new tuple.

4. **Space management**: Searches through the bucket chain (primary page and overflow pages) to find sufficient space. If no space is available, it either:
   - Cleans up dead tuples on pages with cleanup locks
   - Allocates new overflow pages when needed

5. **Tuple insertion and metadata update**: Adds the tuple to the appropriate page, updates the global tuple count in the metapage, and determines if table expansion is needed.

6. **WAL logging**: Records the insertion operation for crash recovery when WAL is enabled.

7. **Table expansion**: Triggers hash table expansion if the load factor threshold is exceeded.

The function includes restart logic to handle cases where bucket splits occur during insertion, ensuring consistency and optimal space utilization.

## Parameters / Member Variables
- : The hash index relation being inserted into
- : The completely filled index tuple to be inserted
- : The heap relation (used for vacuum operations on dead tuples)
- : Boolean flag indicating if inserts are done in hashkey order (optimization hint)

## Dependencies
- Functions called/Symbols referenced:
  - : Extract hash key from tuple
  - : Locate and lock bucket page
  - : Complete bucket split operations
  - : Clean up dead tuples
  - : Allocate new overflow pages
  - : Add tuple to page
  - : Expand hash table when load factor exceeded
  - , : Size and space calculations
  - Various buffer management functions (, , )
  - WAL logging functions (, , etc.)

- Called from (representative examples):
  - : Public interface for single tuple insertion
  - : Bulk loading during index creation
  - : Hash index build process

## Notes and Other Information
- The function uses a restart mechanism ( label) to handle bucket splits that occur during insertion
- Dead tuple cleanup is performed opportunistically when cleanup locks are available
- The load factor check () determines when table expansion is needed
- Critical sections protect the actual tuple insertion and metadata updates to ensure atomicity
- Buffer management carefully distinguishes between primary bucket pages (pin retained) and overflow pages (pin released)
- The  parameter is an optimization hint for bulk loading scenarios where tuples arrive in hash key order