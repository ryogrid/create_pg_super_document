# serializeAnalyzeShutdown

## Location
[src/backend/commands/explain.c:5526-5546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5526-L5546)

## Overview
Cleanly shuts down a SerializeDestReceiver by freeing allocated memory resources including format info arrays, output buffers, and the temporary memory context.

## Definition
```c
static void serializeAnalyzeShutdown(DestReceiver *self)
```

## Detailed Description
This function performs the cleanup phase for a serialize analyze destination receiver. It systematically deallocates all memory resources that were allocated during the receiver's lifetime, including the format info array (finfos), the string buffer data, and the temporary memory context created during startup. The function ensures proper resource cleanup to prevent memory leaks when the destination receiver is no longer needed.

## Parameters / Member Variables
- `self`: Pointer to the DestReceiver being shut down (cast to SerializeDestReceiver)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [SerializeDestReceiver](../S/SerializeDestReceiver.md)
- Called from (representative examples):
  - [CreateExplainSerializeDestReceiver](../C/CreateExplainSerializeDestReceiver.md)

## Notes and Other Information
- This function follows PostgreSQL's standard pattern of setting pointers to NULL after freeing memory
- The cleanup is performed in a safe order: first format info arrays, then buffer data, and finally the memory context
- Each deallocation is protected by a null check to ensure safe cleanup even if some resources were never allocated
- This is the complement to serializeAnalyzeStartup and should be called when the destination receiver is no longer needed
- The function is part of the DestReceiver lifecycle management pattern used throughout PostgreSQL