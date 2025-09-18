# PinPortal

## Location
src/backend/utils/mmgr/portalmem.c: 371 - 379

## Overview
Protects a portal from being dropped by setting its pinned status, preventing accidental or premature portal destruction during active operations.

## Definition
```c
void PinPortal(Portal portal)
```

## Detailed Description
PinPortal is a simple but crucial function in PostgreSQL's portal management system that provides protection against accidental portal destruction. When a portal is pinned, it cannot be dropped through normal cleanup operations, ensuring that it remains available for ongoing operations that depend on it.

The function performs a safety check to prevent double-pinning, which could indicate a programming error or resource management issue. Once pinned, a portal must be explicitly unpinned before it can be dropped.

However, pinning is not absolute protection - pinned portals are still automatically unpinned and dropped during transaction or subtransaction abort operations, ensuring that system recovery can proceed normally even with pinned resources.

## Parameters / Member Variables
- `portal`: The portal to be pinned against dropping

## Dependencies
- Functions called/Symbols referenced:
  - Portal (type)
- Called from (representative examples):
  - plperl_spi_query
  - plperl_spi_query_prepared
  - PLy_cursor_query
  - PLy_cursor_plan

## Notes and Other Information
- Throws an ERROR if the portal is already pinned, preventing double-pinning bugs
- Pinning is automatically cleared during transaction/subtransaction abort for recovery purposes
- Commonly used by procedural language implementations (PL/Perl, PL/Python) to protect cursors during SPI operations
- The pinned status is stored in the portal's portalPinned boolean field
- Must be paired with UnpinPortal() calls to avoid resource leaks in normal execution paths
- Part of the portal reference management system alongside the portal hash table