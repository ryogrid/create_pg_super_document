# RT_RADIX_TREE_CONTROL

## Location
[src/include/lib/radixtree.h:686-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L686-L704)

## Overview
RT_RADIX_TREE_CONTROL is a macro that generates a type name for the control structure containing the core metadata and state information for a PostgreSQL adaptive radix tree instance.

## Definition


## Detailed Description
RT_RADIX_TREE_CONTROL is the central control structure for PostgreSQL's adaptive radix tree implementation. This structure contains all the essential metadata and state information needed to manage a radix tree instance, including the root pointer, size information, and optional debugging statistics.

The control structure is designed to work in both local memory and shared memory configurations. In shared memory mode (RT_SHMEM), it includes additional fields for inter-process coordination such as a handle for DSA (Dynamic Shared Area) management, a magic number for validation, and a lightweight lock for concurrency control.

The structure maintains critical tree state including the current maximum key value, total number of keys stored, and the starting shift level which adapts to the key distribution for optimal tree height. This metadata enables the adaptive behavior that makes the radix tree efficient across different key patterns.

## Parameters / Member Variables
- : (RT_SHMEM only) DSA handle for shared memory allocation management
- : (RT_SHMEM only) Magic number (0x54A48167) for structure validation
- : (RT_SHMEM only) Lightweight lock for concurrent access control
- : Pointer to the root node of the radix tree
- : Maximum key value that can be stored with current tree height
- : Total number of key-value pairs currently stored in the tree
- : Starting bit shift level, adapts to key distribution for optimal tree height
- : (RT_DEBUG only) Array tracking count of nodes for each size class
- : (RT_DEBUG only) Total count of leaf nodes for debugging/statistics

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX  
  - RT_PREFIX
  - RT_HANDLE (in shared memory mode)
  - [LWLock](../L/LWLock.md) (in shared memory mode)
  - RT_PTR_ALLOC
  - RT_NUM_SIZE_CLASSES (for debug arrays)
- Called from (representative examples):
  - [RT_RADIX_TREE](RT_RADIX_TREE.md) (as member field)
  - RT_CREATE
  - [RT_ATTACH](RT_ATTACH.md)

## Notes and Other Information
The RT_RADIX_TREE_CONTROL structure serves as the authoritative source of truth for radix tree state and is critical for several key features:

1. **Adaptive Height Management**: The start_shift field enables "poor man's path compression" by ensuring the tree doesn't contain unnecessary levels where all keys have leading zero bytes.

2. **Shared Memory Support**: The conditional compilation fields (handle, magic, lock) enable safe multi-process access when the tree is allocated in shared memory via DSA.

3. **Concurrency Control**: In shared memory mode, the embedded LWLock provides reader-writer synchronization, allowing multiple concurrent readers or exclusive writers.

4. **Memory Management**: The max_val field tracks the current tree capacity and triggers tree growth when exceeded, while num_keys provides accurate count information.

5. **Debug and Statistics**: The optional debug fields enable detailed tracking of memory usage and node distribution across different size classes, invaluable for performance analysis and optimization.

This structure is typically embedded within the main RT_RADIX_TREE structure and accessed through a ctl pointer, providing a clean separation between the API-level tree object and the core tree state.