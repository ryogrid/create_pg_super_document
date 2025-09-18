# EndCopyTo

## Location
[src/backend/commands/copyto.c:726-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L726-L746)

## Overview
EndCopyTo performs cleanup and resource deallocation for a COPY TO operation, properly releasing all resources associated with the CopyToState structure.

## Definition
```c
void EndCopyTo(CopyToState cstate)
```

## Detailed Description
EndCopyTo is responsible for the orderly shutdown of a COPY TO operation by cleaning up all allocated resources. It handles both query-based and relation-based copy operations by conditionally cleaning up query execution resources when present, and then delegates to EndCopy for general cleanup tasks such as closing files, freeing memory contexts, and updating progress statistics.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing the copy operation state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecutorFinish](ExecutorFinish.md)
  - [ExecutorEnd](ExecutorEnd.md)
  - [FreeQueryDesc](../F/FreeQueryDesc.md)
  - PopActiveSnapshot
  - [EndCopy](EndCopy.md)
- Called from (representative examples):
  - [DoCopy](../D/DoCopy.md)
  - test_copy_to_callback

## Notes and Other Information
The function follows a two-phase cleanup approach: first handling query-specific resources (if applicable) through the executor cleanup functions, then performing general copy operation cleanup through EndCopy. The conditional check for queryDesc ensures that the function works correctly for both relation-based and query-based copy operations. PopActiveSnapshot is called to restore the snapshot stack to its previous state when dealing with query-based copies.