# llvm_release_context

## Location
[src/backend/jit/llvm/llvmjit.c:265-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L265-L334)

## Overview
A static cleanup function responsible for releasing all resources associated with an LLVM JIT compilation context when it's no longer needed.

## Definition

```c
static void
llvm_release_context(JitContext *context)
```
## Detailed Description
This function performs comprehensive cleanup of an LLVM JIT context, including disposing of LLVM modules, cleaning up JIT handles, and managing resource tracking. It implements version-specific cleanup logic for different LLVM versions (>11 vs ≤11) and includes safety measures to avoid cleanup during process exit to prevent potential reentrancy issues with LLVM's error handling.

The function decrements the global usage counter and handles both the LLVM-specific cleanup (modules, execution sessions, symbol pools) and PostgreSQL-specific cleanup (resource owner tracking).

## Parameters / Member Variables
- `*context`: JitContext pointer that gets cast to LLVMJitContext for LLVM-specific cleanup operations
## Dependencies
- Functions called/Symbols referenced:
  - LLVMDisposeModule (LLVM API)
  - LLVMOrcResourceTrackerRemove (LLVM 12+ API)
  - LLVMOrcReleaseResourceTracker (LLVM 12+ API) 
  - LLVMOrcSymbolStringPoolClearDeadEntries (LLVM 12+ API)
  - LLVMOrcRemoveModule (LLVM ≤11 API)
  - [list_free](list_free.md) (PostgreSQL utility)
  - [ResourceOwnerForgetJIT](../R/ResourceOwnerForgetJIT.md) (PostgreSQL resource management)
  - llvm_enter_fatal_on_oom/llvm_leave_fatal_on_oom (LLVM error handling)
- Called from:
  - [_PG_jit_provider_init](../P/_PG_jit_provider_init.md) (as part of JIT provider initialization)

## Notes and Other Information
- Contains version-specific code paths for LLVM major versions >11 vs ≤11
- Skips cleanup during process exit () to avoid potential reentrancy issues
- Manages a global context usage counter ()
- Includes memory leak prevention through symbol string pool cleanup in newer LLVM versions
- Integrates with PostgreSQL's resource owner system for proper resource tracking

## Simplified Source

```c
static void
llvm_release_context(JitContext *context)
{
    LLVMJitContext *llvm_context = (LLVMJitContext *) context;

    // Update usage counter
    llvm_jit_context_in_use_count--;

    // Skip cleanup during process exit to avoid reentrancy issues
    if (proc_exit_inprogress)
        return;

    llvm_enter_fatal_on_oom();

    // Dispose of LLVM module
    if (llvm_context->module) {
        LLVMDisposeModule(llvm_context->module);
        llvm_context->module = NULL;
    }

    // Clean up JIT handles (version-specific logic simplified)
    foreach(lc, llvm_context->handles) {
        LLVMJitHandle *handle = (LLVMJitHandle *) lfirst(lc);
        // Release handle resources based on LLVM version
        pfree(handle);
    }
    list_free(llvm_context->handles);
    llvm_context->handles = NIL;

    llvm_leave_fatal_on_oom();

    // Remove from resource owner tracking
    if (llvm_context->resowner)
        ResourceOwnerForgetJIT(llvm_context->resowner, llvm_context);
}
```