# pltcl_subtrans_commit

## Location
[src/pl/tcl/pltcl.c:2287-2295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2287-L2295)

## Overview
Commits a subtransaction and restores the original transaction context, memory context, and resource owner in PL/Tcl operations.

## Definition
```c
static void pltcl_subtrans_commit(MemoryContext oldcontext, ResourceOwner oldowner)
```

## Detailed Description
This function completes a successful subtransaction by committing the current subtransaction and restoring the execution environment to its state before the subtransaction began. It is the successful completion counterpart to `pltcl_subtrans_begin`. The function ensures that the memory context and resource owner are properly restored to their original values, maintaining proper resource management across subtransaction boundaries.

This function should be called when the operations within the subtransaction have completed successfully and the changes should be committed to the parent transaction.

## Parameters / Member Variables
- `oldcontext`: The original memory context to restore after committing the subtransaction
- `oldowner`: The original resource owner to restore after committing the subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseCurrentSubTransaction
  - MemoryContextSwitchTo (implicitly via context switch)
  - CurrentResourceOwner (global variable assignment)
- Called from (representative examples):
  - pltcl_returnnext
  - pltcl_SPI_prepare

## Notes and Other Information
- Must be preceded by a successful call to `pltcl_subtrans_begin`
- Part of the three-function subtransaction management pattern in PL/Tcl
- Restores both memory context and resource owner to ensure proper cleanup
- Should be called in the successful path of PG_TRY/PG_CATCH blocks
- Located in src/pl/tcl/pltcl.c:2287-2295
- The committed subtransaction's changes become part of the parent transaction