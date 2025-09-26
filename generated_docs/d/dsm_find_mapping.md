# dsm_find_mapping

## Location
[src/backend/storage/ipc/dsm.c:1076-1094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L1076-L1094)

## Overview
Searches for an existing dsm_segment mapping in the current process for a given dsm_handle, returning the segment if found or NULL if not currently mapped.

## Definition
```c
dsm_segment *dsm_find_mapping(dsm_handle handle)
```

## Detailed Description
The dsm_find_mapping function performs a linear search through the process-local dsm_segment_list to locate an existing mapping for the specified handle. This function is useful for determining whether the current process already has a particular dynamic shared memory segment mapped before attempting to attach to it again.

The function uses PostgreSQL's doubly-linked list utilities (dlist_foreach, dlist_container) to iterate through all currently mapped segments in the process. Each segment's handle is compared against the target handle, and the first matching segment is returned.

This is a local operation that only searches within the current process's mappings and does not involve any locks or cross-process communication.

## Parameters / Member Variables
- `handle`: The dsm_handle identifying the segment to search for in the current process's mapping list

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (doubly-linked list iteration macro)
  - dlist_container (container extraction macro)
  - dsm_handle, dsm_segment (types)
  - [dlist_iter](dlist_iter.md) (iterator type)
  - dsm_segment_list (process-local global list)
- Called from (representative examples):
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md) (src/backend/storage/ipc/dsm_registry.c:181)

## Notes and Other Information
- Returns NULL if no mapping exists for the given handle in the current process
- Only searches the current process's local mapping list, not the global segment registry
- Does not create or modify any mappings - purely a lookup function
- Linear search performance: O(n) where n is the number of mapped segments in current process
- Typically used before dsm_attach to avoid redundant attachment attempts
- The returned dsm_segment pointer is valid as long as the mapping remains active in the process