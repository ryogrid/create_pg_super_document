# is_main_region_dsm_handle

## Location
[src/backend/storage/ipc/dsm.c:1281-1288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L1281-L1288)

## Overview
A static inline helper function that determines if a given DSM (Dynamic Shared Memory) handle corresponds to the main region by checking the least significant bit.

## Definition
```c
static inline bool is_main_region_dsm_handle(dsm_handle handle)
```

## Detailed Description
This function implements a simple bitwise check to distinguish between main region DSM handles and other DSM handles. It uses the least significant bit (LSB) as a flag - if the LSB is set (1), the handle refers to a main region; otherwise, it refers to a regular DSM segment. This is an efficient way to encode type information directly into the handle value itself.

## Parameters / Member Variables
- `handle`: A dsm_handle value to be tested for main region identification

## Dependencies
- Functions called/Symbols referenced:
  - dsm_handle (parameter type)
- Called from (representative examples):
  - [dsm_cleanup_using_control_segment](../d/dsm_cleanup_using_control_segment.md)
  - [dsm_postmaster_shutdown](../d/dsm_postmaster_shutdown.md)  
  - [dsm_create](../d/dsm_create.md)
  - [dsm_attach](../d/dsm_attach.md)
  - [dsm_detach](../d/dsm_detach.md)
  - [dsm_pin_segment](../d/dsm_pin_segment.md)
  - [dsm_unpin_segment](../d/dsm_unpin_segment.md)

## Notes and Other Information
This function is marked as static inline for performance optimization since it is called frequently throughout the DSM subsystem. The bitwise check (handle & 1) is a very fast operation that allows PostgreSQL to quickly determine the type of DSM handle without additional lookups or data structures. The main region concept is central to PostgreSQL's dynamic shared memory architecture, where main regions serve as the primary coordination point for shared memory management.

## Simplified Source

```c
// Simplified version of is_main_region_dsm_handle
static inline bool is_main_region_dsm_handle(dsm_handle handle) {
    // Check if least significant bit is set
    return handle & 1;
}
```

Key simplifications made:
- Function is already extremely simple, maintained the essential bitwise check
- Removed inline comments as the operation is self-explanatory
- Preserved the inline optimization for performance