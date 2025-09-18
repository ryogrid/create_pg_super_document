# ResourceOwnerForgetJIT

## Location
src/backend/jit/llvm/llvmjit.c: 152 - 163

## Overview
A convenience wrapper function that unregisters an LLVM JIT context from PostgreSQL's resource owner system, typically called when manually releasing the context.

## Definition
static inline void ResourceOwnerForgetJIT(ResourceOwner owner, LLVMJitContext *handle)

## Detailed Description
ResourceOwnerForgetJIT is a static inline convenience function that wraps the generic ResourceOwnerForget function specifically for LLVM JIT contexts. It removes a previously registered JIT context handle from the resource owner's tracking system. This function is called when a JIT context is being manually released before the resource owner is destroyed, preventing the resource owner from attempting to clean up an already-freed resource.

The function converts the LLVMJitContext pointer to a Datum using PointerGetDatum and removes it from the resource owner using the jit_resowner_desc descriptor.

## Parameters / Member Variables
- owner: The ResourceOwner that was tracking this JIT context
- handle: The LLVMJitContext pointer to be unregistered and removed from tracking

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - PointerGetDatum (macro)
  - jit_resowner_desc (resource descriptor)
- Called from (representative examples):
  - llvm_release_context

## Notes and Other Information
- This is a static inline function, only visible within the llvmjit.c compilation unit
- Companion function to ResourceOwnerRememberJIT for resource management
- Must be called when manually releasing JIT contexts to prevent double-free errors
- Part of PostgreSQL's resource management infrastructure for preventing resource leaks
- Located in src/backend/jit/llvm/llvmjit.c:152-163