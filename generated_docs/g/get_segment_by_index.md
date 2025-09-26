# get_segment_by_index

## Location
src/backend/utils/mmgr/dsa.c: 1757 - 1836

## Overview
Returns the segment map corresponding to a given segment index, lazily mapping the segment into the current process's address space if necessary.

## Definition


## Detailed Description
This function provides access to segment maps within a DSA area, handling the lazy mapping of segments that haven't been accessed yet by the current backend process. When a segment hasn't been mapped (indicated by a NULL mapped_address), the function performs the mapping operation by attaching to the underlying dynamic shared memory (DSM) segment and initializing the segment map structure.

The function is designed to be called in two different locking contexts: with the area lock held for internal segment management, and without locking by dsa_free() and dsa_get_address() functions. The lockless access is safe because callers guarantee they have a live segment index and call check_for_freed_segments() to ensure any freed segments are detached first.

The mapping process involves attaching to the DSM segment using its handle, calculating the addresses of various structures within the segment (header, free page manager, pagemap), and updating the backend's high_segment_index tracker. The function includes assertions to validate the segment's magic number and ensure that freed segments are never returned.

## Parameters / Member Variables
- : The DSA area containing the segment maps and control structures
- : The segment index for which to retrieve or create the segment map

## Dependencies
- Functions called/Symbols referenced:
  - dsm_attach
  - dsm_segment_address
  - elog
- Called from (representative examples):
  - dsa_free
  - dsa_get_address
  - dsa_dump
  - destroy_superblock
  - get_best_segment
  - make_new_segment

## Notes and Other Information
The function handles resource ownership temporarily switching to the area's resource owner during DSM attachment to ensure proper cleanup. It maintains the invariant that mapped segments are never freed, as indicated by the final assertion. The lazy mapping approach optimizes memory usage by only mapping segments that are actually accessed by each backend process.
