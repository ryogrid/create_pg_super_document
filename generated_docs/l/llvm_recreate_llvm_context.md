# llvm_recreate_llvm_context

## Location
[src/backend/jit/llvm/llvmjit.c:186-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L186-L235)

## Overview
A memory management function that periodically recreates the LLVM context to prevent memory accumulation from type leaks during inlining operations.

## Definition
static void llvm_recreate_llvm_context(void)

## Detailed Description
llvm_recreate_llvm_context is a critical memory management function that addresses a specific LLVM memory leak issue. During inlining operations, LLVM may "leak" types - they remain findable via the context but new types are created in subsequent inlining rounds, slowly accumulating problematic amounts of memory.

The function implements a heuristic-based approach using a reuse counter (LLVMJIT_LLVM_CONTEXT_REUSE_MAX) to determine when to recreate the context. It includes several safety checks:
- Verifies that a context exists before attempting recreation
- Ensures no other code is currently being JITed to avoid releasing types in use
- Resets cached modules before disposing the context to prevent dangling pointers
- Recreates type information after context recreation

The function follows a careful sequence: reset inline caches → dispose old context → create new context → reset counter → rebuild type information.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - llvm_inline_reset_caches (clears module caches)
  - LLVMContextDispose (LLVM API to dispose context)
  - LLVMContextCreate (LLVM API to create new context)  
  - [llvm_create_types](llvm_create_types.md) (rebuilds type information)
  - LLVMJIT_LLVM_CONTEXT_REUSE_MAX (constant defining reuse threshold)
- Called from (representative examples):
  - [llvm_create_context](llvm_create_context.md)

## Notes and Other Information
- Static function, only accessible within llvmjit.c
- Uses global variables: llvm_context, llvm_jit_context_in_use_count, llvm_llvm_context_reuse_count
- Future improvement mentioned in comments: make this more fine-grained, only recreate when inlining actually occurred
- Alternative improvement: use LLVM context size rather than usage count heuristic
- Critical for long-running PostgreSQL instances that perform many JIT compilations
- Located in src/backend/jit/llvm/llvmjit.c:186-235