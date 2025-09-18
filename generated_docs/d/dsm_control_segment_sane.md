# dsm_control_segment_sane

## Location
src/backend/storage/ipc/dsm.c: 1237 - 1254

## Overview
Performs sanity checks on a DSM control segment to verify its basic integrity and ensure safe iteration over its contents without memory access violations.

## Definition
```c
static bool dsm_control_segment_sane(dsm_control_header *control, Size mapped_size)
```

## Detailed Description
This internal validation function performs essential sanity checks on a DSM control segment header to ensure it is safe to access. The function is designed to prevent crashes and memory access violations rather than perform comprehensive validation. It verifies four critical aspects:

1. **Mapping Size**: Ensures the mapped size is at least large enough to contain the basic header structure
2. **Magic Number**: Validates that the control segment contains the expected magic number (PG_DYNSHMEM_CONTROL_MAGIC)
3. **Capacity Consistency**: Verifies that the maximum item count can fit within the mapped memory size
4. **Item Count Bounds**: Ensures the current item count doesn't exceed the maximum allowed items

The function is intentionally conservative - it checks only what's necessary to prevent crashes during segment iteration, not comprehensive data integrity.

## Parameters / Member Variables
- `control`: Pointer to the DSM control header structure to validate
- `mapped_size`: Size of the mapped memory region containing the control segment

## Dependencies
- Functions called/Symbols referenced:
  - offsetof (macro)
  - [dsm_control_bytes_needed](dsm_control_bytes_needed.md)
  - PG_DYNSHMEM_CONTROL_MAGIC
- Called from (representative examples):
  - dsm_cleanup_using_control_segment
  - dsm_postmaster_shutdown
  - dsm_backend_startup

## Notes and Other Information
- Static function - internal to dsm.c implementation
- Designed for crash prevention rather than comprehensive validation
- Returns true if the segment passes basic sanity checks, false otherwise
- Critical for safe iteration over DSM control segment items
- Located in src/backend/storage/ipc/dsm.c:1237-1254