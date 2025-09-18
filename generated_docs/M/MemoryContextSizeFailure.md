# MemoryContextSizeFailure

## Location
[src/backend/utils/mmgr/mcxt.c:1167-1179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1167-L1179)

## Overview
MemoryContextSizeFailure handles invalid memory allocation request sizes in PostgreSQL memory contexts, providing a standardized error response for size validation failures.

## Definition
```c
void MemoryContextSizeFailure(MemoryContext context, Size size, int flags)
```

## Detailed Description
This function serves as a centralized handler for invalid memory allocation size requests across all memory context implementations. It provides a consistent error response when memory context implementations detect that a requested allocation size is invalid or out of bounds.

The function simply logs an ERROR with the invalid size value and terminates the current operation. Unlike MemoryContextAllocationFailure, this function does not check allocation flags because size validation failures are always considered programming errors that should not be handled gracefully.

This function is part of the memory context infrastructure used by various memory context method implementations to maintain consistent error handling across different allocation strategies.

## Parameters / Member Variables
- `context`: The memory context in which the size validation failure occurred (currently unused in implementation)
- `size`: The invalid allocation size that triggered the failure
- `flags`: Allocation flags (currently unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error logging and termination)
- Called from (representative examples):
  - Memory context method implementations (when size validation fails)
  - Internal allocation routines that detect invalid size parameters

## Notes and Other Information
- This function is intended for use only by MemoryContextMethods implementations, not general application code
- The function always raises an ERROR and never returns normally
- Unlike allocation failures, size failures are treated as programming errors that cannot be handled gracefully
- The context and flags parameters are currently unused but maintained for API consistency with other failure handlers
- Size validation typically catches issues like negative sizes, extremely large sizes, or sizes that would cause integer overflow
- This function helps maintain robust error handling across PostgreSQL's various memory context implementations