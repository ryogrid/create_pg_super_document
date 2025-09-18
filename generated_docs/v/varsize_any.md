# varsize_any

## Location
src/backend/access/common/heaptuple.c: 1594 - 1597

## Overview
A wrapper function that returns the size of a variable-length data structure, primarily designed to enable JIT inlining of the VARSIZE_ANY macro.

## Definition
```c
size_t varsize_any(void *p)
```

## Detailed Description
This function serves as a callable wrapper around the VARSIZE_ANY macro, which determines the size of variable-length (varlena) data structures in PostgreSQL. The primary purpose of this function is to provide a callable interface that can be efficiently inlined by the JIT compiler, as macros cannot be directly called from JIT-compiled code.

The function is also useful during debugging sessions where you need to examine the size of varlena structures interactively, as you can call this function directly from a debugger rather than having to manually expand the macro.

## Parameters / Member Variables
- `p`: A void pointer to a variable-length data structure (typically a varlena structure) whose size needs to be determined.

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE_ANY (macro)

- Called from (representative examples):
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in JIT context)
  - HeapTupleClearHeapOnly
  - Used in debugging scenarios

## Notes and Other Information
- The function exists primarily for JIT compilation support, as the JIT compiler needs callable functions rather than preprocessor macros
- Provides a convenient debugging interface for examining varlena structure sizes
- The underlying VARSIZE_ANY macro handles the complexity of determining sizes for various varlena formats (compressed, external, etc.)
- Returns the total size including any varlena header information
- This is a thin wrapper with minimal overhead - the actual size computation is performed by the VARSIZE_ANY macro