# BumpRealloc

## Location
[src/backend/utils/mmgr/bump.c:627-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L627-L637)

## Overview
A deliberately unsupported function that throws an error when called, enforcing the bump allocator's design constraint that allocated memory cannot be resized in-place.

## Definition


## Detailed Description
BumpRealloc is an intentionally non-functional implementation of the memory context reallocation operation for the bump allocator. Instead of attempting to resize the memory pointed to by the given pointer, this function immediately throws an ERROR indicating that 'realloc is not supported by the bump memory allocator'. This design choice is consistent with bump allocator principles: once memory is allocated sequentially, it cannot be individually managed, resized, or freed. The function returns NULL after the error to satisfy compiler requirements, though this return statement is never reached due to the error.

## Parameters / Member Variables
- : Pointer to the memory block to be reallocated (parameter is ignored as the function always errors)
- : The new size requested for the memory block (parameter is ignored)
- : Flags controlling reallocation behavior (parameter is ignored)

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging function)
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer in memory context methods)
  - Referenced in MEMUTILS_INTERNAL_H header

## Notes and Other Information
- This function is part of the MemoryContextMethods function pointer table for bump contexts
- The error message specifically mentions 'realloc' to clearly indicate that memory resizing is not supported
- The 'keep compiler quiet' comment explains why NULL is returned after the error (unreachable code)
- Consistent with bump allocator design philosophy where memory management is simplified by eliminating individual allocation manipulation
- Applications requiring dynamic resizing must use different memory context types or implement resizing through allocation of new memory and copying data