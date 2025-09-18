# gistinsert

## Location
src/backend/access/gist/gist.c: 159 - 224

## Overview
The public interface routine for tuple insertion in GiST indexes, serving as a wrapper that manages state initialization and delegates the actual insertion work to lower-level functions.

## Definition


## Detailed Description
This function serves as the main entry point for inserting tuples into GiST indexes. It acts as a wrapper that handles state management and memory context switching before delegating the actual insertion work to . The function initializes the GISTSTATE cache on the first call within a statement, creates a temporary memory context for safe operation, forms an index tuple from the provided values, and manages memory cleanup after the insertion.

The function ensures proper memory management by using a temporary context and resetting it after each insertion to prevent memory leaks. It also maintains the GISTSTATE cache in the index's memory context for reuse across multiple insertions within the same statement.

## Parameters / Member Variables
- : The GiST index relation to insert into
- : Array of Datum values for the index tuple
- : Array of boolean flags indicating null values
- : Item pointer to the heap tuple being indexed
- : The heap relation containing the tuple
- : Uniqueness checking mode (not used in GiST)
- : Flag indicating if index values changed (optimization hint)
- : Index information structure containing cached state

## Dependencies
- Functions called/Symbols referenced:
  - [initGISTstate](../i/initGISTstate.md) (initializes GiST state structure)
  - [createTempGistContext](../c/createTempGistContext.md) (creates temporary memory context)
  - gistFormTuple (forms index tuple from values)
  - [gistdoinsert](gistdoinsert.md) (performs the actual insertion)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (switches memory contexts)
  - [MemoryContextReset](../M/MemoryContextReset.md) (resets temporary context)
- Types used:
  - [GISTSTATE](../G/GISTSTATE.md) (GiST state structure)
  - [IndexTuple](../I/IndexTuple.md) (index tuple structure)
  - IndexInfo (index information structure)
  - IndexUniqueCheck (uniqueness checking enumeration)
- Called from:
  - [gisthandler](gisthandler.md) (assigned as aminsert callback at src/backend/access/gist/gist.c:88)

## Notes and Other Information
- Always returns false since GiST doesn't support unique indexes
- The function initializes and caches GISTSTATE on first use within a statement for performance
- Uses a temporary memory context to ensure safe memory management during insertion
- The actual insertion logic is delegated to  function
- Memory context is reset after each insertion to prevent memory accumulation
- Located in src/backend/access/gist/gist.c:159-224