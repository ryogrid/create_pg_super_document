# JitProviderCallbacks

## Location
[src/include/jit/jit.h:65-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/jit.h#L65-L74)

## Overview
JitProviderCallbacks defines the interface structure that JIT provider implementations must implement to integrate with PostgreSQL's generic JIT framework.

## Definition

```c
typedef struct JitProviderCallbacks JitProviderCallbacks;
```
## Detailed Description
JitProviderCallbacks serves as the plugin interface for JIT compilation providers in PostgreSQL. It defines a set of function pointers that concrete JIT implementations (such as the LLVM provider) must implement to provide JIT compilation services. This structure enables PostgreSQL to support multiple JIT backends through a common interface, allowing the core system to remain provider-agnostic while delegating the actual JIT compilation work to specialized implementations.

## Parameters / Member Variables
- : Function pointer for cleanup operations after JIT compilation errors, takes no parameters
- : Function pointer for releasing JIT context resources, takes a JitContext pointer parameter
- : Function pointer for compiling expression trees, takes an ExprState pointer and returns bool indicating success

## Dependencies
- Functions called/Symbols referenced:
  - JitProviderResetAfterErrorCB (function pointer type)
  - JitProviderReleaseContextCB (function pointer type)
  - JitProviderCompileExprCB (function pointer type)
  - [JitContext](JitContext.md) (parameter type for release_context)
  - ExprState (parameter type for compile_expr)
  - [_PG_jit_provider_init](../P/_PG_jit_provider_init.md) (initialization function that receives this structure)
- Called from (representative examples):
  - [_PG_jit_provider_init](../P/_PG_jit_provider_init.md) (in LLVM provider)

## Notes and Other Information
- This is the primary extension point for implementing new JIT providers in PostgreSQL
- JIT providers must implement all callback functions to provide a complete JIT implementation
- Used in conjunction with _PG_jit_provider_init function which JIT providers must export
- The interface supports both expression compilation and context management operations
- Enables pluggable JIT architecture where different compilation backends can be used interchangeably
- Related to the processed symbol _PG_jit_provider_init which initializes these callbacks