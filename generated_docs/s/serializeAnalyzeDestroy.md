# serializeAnalyzeDestroy

## Location
src/backend/commands/explain.c: 5547 - 5555

## Overview
Finalizes the destruction of a SerializeDestReceiver by freeing the receiver structure itself after all cleanup has been completed.

## Definition
```c
static void serializeAnalyzeDestroy(DestReceiver *self)
```

## Detailed Description
This function performs the final step in the SerializeDestReceiver lifecycle by deallocating the receiver structure itself. It is called after serializeAnalyzeShutdown has cleaned up all internal resources. The function simply frees the memory occupied by the DestReceiver structure, completing the destruction process. This follows PostgreSQL's standard pattern where shutdown handles resource cleanup and destroy handles structure deallocation.

## Parameters / Member Variables
- `self`: Pointer to the DestReceiver structure to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CreateExplainSerializeDestReceiver](../C/CreateExplainSerializeDestReceiver.md)

## Notes and Other Information
- This is the final step in the DestReceiver lifecycle: startup -> [use] -> shutdown -> destroy
- The function assumes that serializeAnalyzeShutdown has already been called to clean up internal resources
- This follows the standard PostgreSQL pattern where destroy functions only handle the final structure deallocation
- The function is minimal by design - all complex cleanup should be handled in the shutdown function
- Part of the DestReceiver interface pattern used throughout PostgreSQL for managing query output destinations