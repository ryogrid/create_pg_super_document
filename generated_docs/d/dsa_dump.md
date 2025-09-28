# dsa_dump

## Location
[src/backend/utils/mmgr/dsa.c:1088-1195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1088-L1195)

## Overview
Prints detailed debugging information about the internal state of a dynamic shared memory area to stderr for diagnostic purposes.

## Definition
```c
void dsa_dump(dsa_area *area)
```

## Detailed Description
This function provides comprehensive debugging output that reveals the internal structure and state of a DSA area. It prints information about the area's overall configuration, segment allocation status, and detailed pool statistics across all size classes.

The function produces an inconsistent snapshot since it acquires and releases individual locks as it traverses different data structures rather than holding all locks simultaneously. This approach avoids potential deadlocks but means the output may show a slightly inconsistent view if the area is being modified concurrently.

The output includes:
- Area handle and size limits
- Reference count and pinned status  
- Segment bin information showing free page availability
- Detailed pool information for each size class including fullness statistics
- Individual span details within each fullness class

## Parameters / Member Variables
- `area`: Pointer to the dynamic shared memory area to dump debugging information for

## Dependencies
- Functions called/Symbols referenced:
  - DSA_AREA_LOCK
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [check_for_freed_segments_locked](../c/check_for_freed_segments_locked.md)
  - [get_segment_by_index](../g/get_segment_by_index.md)
  - fpm_largest
  - DSA_SCLASS_LOCK
  - DsaPointerIsValid
  - [dsa_get_address](dsa_get_address.md)
  - fprintf
- Called from (representative examples):
  - Available through DSA public interface for debugging

## Notes and Other Information
- Outputs debugging information to stderr using fprintf
- Creates an inconsistent snapshot due to incremental lock acquisition
- Shows segment bins with contiguous free page counts
- Displays detailed span information including object allocation ratios
- Uses special formatting constants like DSA_POINTER_FORMAT for consistent output
- Helpful for diagnosing memory fragmentation and allocation patterns
- Should only be used for debugging purposes due to performance impact
- The pinned status indicates whether the area is pinned in memory

## Simplified Source

```c
// Simplified version of dsa_dump
void
dsa_dump(dsa_area *area) {
    size_t i, j;

    // Phase 1: Dump area-level information
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    check_for_freed_segments_locked(area);

    fprintf(stderr, "dsa_area handle %x:\n", area->control->handle);
    fprintf(stderr, "  max_total_segment_size: %zu\n", area->control->max_total_segment_size);
    fprintf(stderr, "  total_segment_size: %zu\n", area->control->total_segment_size);
    fprintf(stderr, "  refcnt: %d\n", area->control->refcnt);
    fprintf(stderr, "  pinned: %c\n", area->control->pinned ? 't' : 'f');

    // Phase 2: Dump segment bin information
    fprintf(stderr, "  segment bins:\n");
    for (i = 0; i < DSA_NUM_SEGMENT_BINS; ++i) {
        if (area->control->segment_bins[i] != DSA_SEGMENT_INDEX_NONE) {
            dump_segment_bin(area, i);
        }
    }
    LWLockRelease(DSA_AREA_LOCK(area));

    // Phase 3: Dump pool information for each size class
    fprintf(stderr, "  pools:\n");
    for (i = 0; i < DSA_NUM_SIZE_CLASSES; ++i) {
        LWLockAcquire(DSA_SCLASS_LOCK(area, i), LW_EXCLUSIVE);

        if (has_spans_in_any_fullness_class(area, i)) {
            dump_size_class_pool(area, i);

            // Dump each fullness class within this size class
            for (j = 0; j < DSA_FULLNESS_CLASSES; ++j) {
                dump_fullness_class(area, i, j);
            }
        }

        LWLockRelease(DSA_SCLASS_LOCK(area, i));
    }
}

// Helper function concepts (simplified representations)
void dump_segment_bin(dsa_area *area, size_t bin_index) {
    // Iterate through segment chain and print segment details
    dsa_segment_index segment_index = area->control->segment_bins[bin_index];
    while (segment_index != DSA_SEGMENT_INDEX_NONE) {
        dsa_segment_map *segment_map = get_segment_by_index(area, segment_index);
        fprintf(stderr, "      segment index %zu, usable_pages = %zu, mapped at %p\n",
                segment_index, segment_map->header->usable_pages, segment_map->mapped_address);
        segment_index = segment_map->header->next;
    }
}

void dump_fullness_class(dsa_area *area, size_t size_class, size_t fullness_class) {
    // Walk span chain and print span details
    dsa_pointer span_pointer = area->control->pools[size_class].spans[fullness_class];
    while (DsaPointerIsValid(span_pointer)) {
        dsa_area_span *span = dsa_get_address(area, span_pointer);
        fprintf(stderr, "        span at " DSA_POINTER_FORMAT ", objects free = %hu/%hu\n",
                span_pointer, span->nallocatable, span->nmax);
        span_pointer = span->nextspan;
    }
}
```

Key simplifications made:
- Broke down the function into clear phases with comments
- Abstracted repetitive debugging output into conceptual helper functions
- Simplified the nested loops while preserving the core structure
- Emphasized the incremental locking strategy
- Focused on the core pattern: lock → dump info → unlock for each major data structure