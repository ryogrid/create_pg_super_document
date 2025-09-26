# ResourceOwnerRememberJIT

## Location
[src/backend/jit/llvm/llvmjit.c:147-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L147-L151)

## Overview
A convenience wrapper function that registers an LLVM JIT context with PostgreSQL's resource owner system for proper cleanup management.

## Definition

```c
static inline void
ResourceOwnerRememberJIT(ResourceOwner owner, LLVMJitContext *handle)
```
## Detailed Description
ResourceOwnerRememberJIT is a static inline convenience function that wraps the generic ResourceOwnerRemember function specifically for LLVM JIT contexts. It registers a JIT context handle with the resource owner system, ensuring that the JIT context will be properly cleaned up when the resource owner is released. This is part of PostgreSQL's resource management infrastructure that prevents resource leaks by automatically cleaning up resources when transactions abort or sessions end.

The function converts the LLVMJitContext pointer to a Datum using PointerGetDatum and associates it with the jit_resowner_desc descriptor, which defines the cleanup callbacks for JIT resources.

## Parameters / Member Variables
- : The ResourceOwner that will track this JIT context for cleanup
- : The LLVMJitContext pointer to be registered and tracked

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [PointerGetDatum](../P/PointerGetDatum.md) (macro)
  - jit_resowner_desc (resource descriptor)
- Called from (representative examples):
  - [llvm_create_context](../l/llvm_create_context.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the llvmjit.c compilation unit
- Part of a pair with ResourceOwnerForgetJIT for resource management
- The jit_resowner_desc provides the cleanup callbacks needed when the resource owner is released
- Essential for preventing memory leaks in JIT compilation contexts
- Located in src/backend/jit/llvm/llvmjit.c:147-151