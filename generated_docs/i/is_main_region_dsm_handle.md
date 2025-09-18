# is_main_region_dsm_handle

## Location
src/backend/storage/ipc/dsm.c: 1281 - 1288

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
  - dsm_cleanup_using_control_segment
  - dsm_postmaster_shutdown  
  - dsm_create
  - dsm_attach
  - dsm_detach
  - dsm_pin_segment
  - dsm_unpin_segment

## Notes and Other Information
This function is marked as static inline for performance optimization since it is called frequently throughout the DSM subsystem. The bitwise check (handle & 1) is a very fast operation that allows PostgreSQL to quickly determine the type of DSM handle without additional lookups or data structures. The main region concept is central to PostgreSQL's dynamic shared memory architecture, where main regions serve as the primary coordination point for shared memory management.