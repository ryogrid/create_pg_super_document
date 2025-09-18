# BumpFree

## Location
src/backend/utils/mmgr/bump.c: 617 - 626

## Overview
A deliberately unsupported function that throws an error when called, enforcing the bump allocator's design principle that individual allocations cannot be freed.

## Definition


## Detailed Description
BumpFree is an intentionally non-functional implementation of the memory context free operation for the bump allocator. Rather than performing any actual memory deallocation, this function immediately throws an ERROR with the message that 'pfree is not supported by the bump memory allocator'. This design choice reflects the fundamental philosophy of bump allocators: memory is allocated sequentially and freed only when the entire context is reset or destroyed. Individual chunk deallocation would violate the allocator's efficiency guarantees and simple implementation model.

## Parameters / Member Variables
- : Pointer to memory that would be freed (parameter is ignored as the function always errors)

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging function)
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer in memory context methods)
  - Referenced in MEMUTILS_INTERNAL_H header

## Notes and Other Information
- This function is part of the MemoryContextMethods function pointer table for bump contexts
- The error message specifically mentions 'pfree' to clearly indicate to developers that the standard PostgreSQL free function is not supported
- This design pattern is common in specialized allocators where individual deallocation would compromise performance or simplicity
- Developers using bump contexts must rely on context reset or destruction to free memory
- The bump allocator is optimized for scenarios with many small, short-lived allocations that don't need individual cleanup