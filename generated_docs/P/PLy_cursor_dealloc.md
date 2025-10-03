# PLy_cursor_dealloc

## Location
[src/pl/plpython/plpy_cursorobject.c:277-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_cursorobject.c#L277-L303)

## Overview
Deallocates a PL/Python cursor object, properly closing the associated PostgreSQL portal and cleaning up allocated memory contexts.

## Definition

```c
static void
PLy_cursor_dealloc(PyObject *arg)
```
## Detailed Description
PLy_cursor_dealloc serves as the Python tp_dealloc function for PLyCursorObject instances, implementing proper cleanup when a cursor object is garbage collected or explicitly deleted. The function performs comprehensive resource management by ensuring the PostgreSQL portal is properly closed and unpinned, the cursor's dedicated memory context is deleted, and the Python object itself is freed.

The function first checks if the cursor is still open and, if so, retrieves the portal by name and validates it before unpinning and closing it through SPI_cursor_close(). This ensures that PostgreSQL's portal management system is properly notified and resources are released. After portal cleanup, it deletes the cursor's memory context, which automatically frees all memory allocated within that context during the cursor's lifetime.

The function follows Python's object deallocation protocol by calling the type's tp_free function to release the Python object structure itself.

## Parameters / Member Variables
- `*arg`: PyObject pointer to the PLyCursorObject being deallocated
## Dependencies
- Functions called/Symbols referenced:
  - [GetPortalByName](../G/GetPortalByName.md)
  - PortalIsValid
  - [UnpinPortal](../U/UnpinPortal.md)
  - [SPI_cursor_close](../S/SPI_cursor_close.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - tp_free (Python C API via ob_type)
- Called from (representative examples):
  - Python garbage collector (referenced in PLy_CursorType.tp_dealloc)

## Notes and Other Information
- Registered as the tp_dealloc function in the PLy_CursorType Python type object definition
- Ensures proper cleanup even if the cursor was not explicitly closed by user code
- Uses defensive programming by checking cursor->closed state and portal validity before cleanup
- The portal unpinning is critical as portals are pinned during creation to prevent premature cleanup
- Memory context deletion automatically frees all allocations made within that context during cursor lifetime
- Sets cursor->mcxt to NULL after deletion to prevent double-free scenarios
- Part of Python's automatic memory management system for PL/Python cursor objects