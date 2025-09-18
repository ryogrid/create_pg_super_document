# UnpinPortal

## Location
src/backend/utils/mmgr/portalmem.c: 380 - 394

## Overview
Removes the pinned protection from a portal, allowing it to be dropped again through normal cleanup operations.

## Definition
```c
void UnpinPortal(Portal portal)
```

## Detailed Description
UnpinPortal is the counterpart to PinPortal(), removing the protection against portal dropping by clearing the portal's pinned status. This function is essential for proper resource management, ensuring that portals can be cleaned up once they are no longer needed by the operations that pinned them.

The function includes a safety check to ensure that the portal is actually pinned before attempting to unpin it. This helps catch programming errors where UnpinPortal() is called without a corresponding PinPortal() call, or where the portal has already been unpinned.

Once unpinned, the portal becomes eligible for normal cleanup operations and can be dropped when appropriate.

## Parameters / Member Variables
- `portal`: The portal to be unpinned, which must currently be in pinned state

## Dependencies
- Functions called/Symbols referenced:
  - Portal (type)
- Called from (representative examples):
  - plperl_spi_fetchrow
  - plperl_spi_cursor_close
  - PLy_cursor_dealloc
  - PLy_cursor_close

## Notes and Other Information
- Throws an ERROR if the portal is not currently pinned, helping detect resource management bugs
- Must be called for every PinPortal() call to avoid resource leaks
- Commonly used by procedural language implementations to release portal protection after SPI operations complete
- The function sets the portal's portalPinned field to false
- Used in cursor cleanup operations in both PL/Perl and PL/Python
- Critical for preventing portal resource leaks in procedural language contexts
- Does not actually drop the portal - only removes the protection against dropping