# fillFakeState

## Location
src/backend/access/spgist/spgxlog.c: 35 - 49

## Overview
Prepares a minimal SpGistState structure with only the essential information needed for WAL replay operations in SP-GiST indexes.

## Definition
```c
static void fillFakeState(SpGistState *state, spgxlogState stateSrc)
```

## Detailed Description
This function creates a "dummy" SpGistState structure that contains the minimum information required for SP-GiST WAL replay operations. It initializes the state structure by zeroing out all fields and then selectively populating only the fields that are essential for replay functionality:

- The transaction ID for redirected tuples (redirectXid)
- A flag indicating whether this is part of an index build operation (isBuild)  
- Storage space for dead tuple information (deadTupleStorage)

The function is designed to be lightweight and efficient, avoiding the overhead of creating a full SpGistState structure during recovery operations where most state information is not needed.

## Parameters / Member Variables
- `state`: Pointer to the SpGistState structure to be initialized for replay operations
- `stateSrc`: Source spgxlogState structure containing the minimal state information from the WAL record

## Dependencies
- Functions called/Symbols referenced:
  - memset (standard C library function)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - SpGistState (structure type)
  - [spgxlogState](../s/spgxlogState.md) (structure type)
  - SGDTSIZE (macro for dead tuple storage size)
- Called from (representative examples):
  - [spgRedoMoveLeafs](../s/spgRedoMoveLeafs.md)
  - [spgRedoAddNode](../s/spgRedoAddNode.md)
  - [spgRedoPickSplit](../s/spgRedoPickSplit.md)
  - [spgRedoVacuumLeaf](../s/spgRedoVacuumLeaf.md)

## Notes and Other Information
- This is a static function used only within the SP-GiST WAL replay module (spgxlog.c)
- The function supports spgFormDeadTuple() operations during replay
- Memory allocated for deadTupleStorage is zeroed using palloc0 for safety
- The function is part of PostgreSQL's Write-Ahead Logging (WAL) recovery system for SP-GiST indexes