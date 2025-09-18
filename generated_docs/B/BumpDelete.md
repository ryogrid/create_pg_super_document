# BumpDelete

## Location
[src/backend/utils/mmgr/bump.c:278-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L278-L292)

## Overview
Completely destroys a Bump memory context by freeing all allocated memory and the context structure itself.

## Definition


## Detailed Description
BumpDelete performs the complete destruction of a Bump memory context. It operates in two phases: first calling BumpReset to free all memory blocks except the keeper block, then freeing the context structure itself (which includes the keeper block). This two-step approach ensures that all allocated memory is properly released before the context header is deallocated.

The function is straightforward and efficient, leveraging the BumpReset functionality to handle the bulk of the cleanup work. After reset, only the initial allocation (containing both the context header and keeper block) remains, which is then freed with a single call to free().

## Parameters / Member Variables
- `context`: The Bump memory context to delete

## Dependencies
- Functions called/Symbols referenced:
  - [BumpReset](BumpReset.md)
  - free
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer)

## Notes and Other Information
- The function follows the standard pattern for context deletion: reset then free the context structure
- Since the initial block contains both the context header and keeper block, a single free() call releases both
- After this function completes, the context pointer becomes invalid and should not be used
- This is the final cleanup function in the lifecycle of a Bump memory context