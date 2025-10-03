# PQfreeCancel

## Location
[src/interfaces/libpq/fe-cancel.c:418-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L418-L431)

## Overview
Frees memory allocated for a PostgreSQL cancel structure, releasing resources associated with a cancel request object.

## Definition

```c
void
PQfreeCancel(PGcancel *cancel)
```
## Detailed Description
PQfreeCancel is a simple memory management function in the libpq library that deallocates a PGcancel structure. This function should be called when a cancel request object is no longer needed to prevent memory leaks. The function simply calls the standard library's free() function to release the memory pointed to by the cancel parameter.

## Parameters / Member Variables
- `*cancel`: Pointer to the PGcancel structure to be freed. This should be a valid pointer previously allocated by PostgreSQL's cancel request creation functions.
## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - [PGcancel](PGcancel.md) (structure type)
- Called from (representative examples):
  - [set_archive_cancel_info](../s/set_archive_cancel_info.md) (src/bin/pg_dump/parallel.c:757)
  - [SetCancelConn](../S/SetCancelConn.md) (src/fe_utils/cancel.c:92)
  - [ResetCancelConn](../R/ResetCancelConn.md) (src/fe_utils/cancel.c:121)
  - [PQrequestCancel](PQrequestCancel.md) (src/interfaces/libpq/fe-cancel.c:687)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:277)

## Notes and Other Information
- This function performs no validation on the input pointer, so passing NULL or an invalid pointer will result in undefined behavior
- Should always be paired with the creation of PGcancel structures to ensure proper memory management
- Part of the libpq public API for PostgreSQL client applications
- Location: src/interfaces/libpq/fe-cancel.c:418-431