# llvm_create_context

## Location
[src/backend/jit/llvm/llvmjit.c:236-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L236-L264)

## Overview
Creates and initializes a new LLVM JIT compilation context with proper resource management and cleanup registration.

## Definition
LLVMJitContext *llvm_create_context(int jitFlags)

## Detailed Description
llvm_create_context is the main function for creating LLVM JIT compilation contexts in PostgreSQL. It performs comprehensive initialization including session setup, context recreation for memory management, resource owner registration, and proper cleanup handling.

The function follows a careful initialization sequence:
1. Validates that it's being called in a safe context (fatal section)
2. Initializes the LLVM session if needed
3. Recreates the LLVM context if necessary (for memory management)
4. Enlarges the resource owner capacity
5. Allocates the context structure in TopMemoryContext for longevity
6. Sets the JIT flags specified by the caller
7. Registers the context with the current resource owner for cleanup
8. Increments the global usage counter

The context will be automatically cleaned up when the CurrentResourceOwner is destroyed (typically at transaction end) or when explicitly released.

## Parameters / Member Variables
- jitFlags: Integer flags controlling JIT compilation behavior and options

## Dependencies
- Functions called/Symbols referenced:
  - llvm_assert_in_fatal_section (validates safe calling context)
  - [llvm_session_initialize](llvm_session_initialize.md) (initializes LLVM session)
  - [llvm_recreate_llvm_context](llvm_recreate_llvm_context.md) (manages LLVM context lifecycle)
  - ResourceOwnerEnlarge (expands resource tracking capacity)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (allocates zeroed memory)
  - [ResourceOwnerRememberJIT](../R/ResourceOwnerRememberJIT.md) (registers context for cleanup)
- Called from (representative examples):
  - llvm_compile_expr

## Notes and Other Information
- Allocates context in TopMemoryContext to ensure it survives across memory context resets
- Uses CurrentResourceOwner for automatic cleanup on transaction abort/commit
- Increments llvm_jit_context_in_use_count to track active contexts
- Part of PostgreSQL's JIT compilation infrastructure
- Must be called within a fatal section for safety
- Returns a pointer to the newly created LLVMJitContext structure
- Located in src/backend/jit/llvm/llvmjit.c:236-264