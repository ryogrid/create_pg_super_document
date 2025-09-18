# PLy_spi_subtransaction_commit

## Location
src/pl/plpython/plpy_spi.c: 577 - 585

## Overview
PLy_spi_subtransaction_commit commits an internal subtransaction within PL/Python and restores the previous execution context, completing the successful path of subtransaction-protected SPI operations.

## Definition
void PLy_spi_subtransaction_commit(MemoryContext oldcontext, ResourceOwner oldowner)

## Detailed Description
PLy_spi_subtransaction_commit finalizes a successfully completed subtransaction by calling ReleaseCurrentSubTransaction() to commit the changes made within the subtransaction. After committing, it restores the caller's original memory context and resource owner, effectively returning control to the outer transaction scope. This function is the successful completion counterpart to PLy_spi_subtransaction_begin and should be called when SPI operations within the subtransaction have completed without errors.

## Parameters / Member Variables
- oldcontext: The original memory context to restore after committing the subtransaction
- oldowner: The original resource owner to restore after committing the subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseCurrentSubTransaction: PostgreSQL core function to commit and release the current subtransaction
  - MemoryContextSwitchTo: Switches back to the original memory context
  - CurrentResourceOwner: Global variable set to restore the original resource owner
- Called from (representative examples):
  - PLy_cursor_query: After successful cursor query execution
  - PLy_cursor_plan: After successful cursor plan operations
  - PLy_spi_prepare: After successful SPI query preparation
  - PLy_spi_execute_plan: After successful prepared plan execution
  - PLy_spi_execute_query: After successful direct query execution

## Notes and Other Information
- Must be paired with a preceding PLy_spi_subtransaction_begin call
- Should only be called in the success path of PG_TRY blocks - errors should use PLy_spi_subtransaction_abort instead
- Restores both memory context and resource owner to maintain proper resource management
- The subtransaction changes become part of the outer transaction after this call
- Unlike the abort function, this does not set Python exceptions since the operation succeeded
- Part of the three-function subtransaction management suite (begin/commit/abort)