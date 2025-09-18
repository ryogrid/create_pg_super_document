# llvm_release_context

## Location
[src/backend/jit/llvm/llvmjit.c:265-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L265-L334)

## Overview
A static cleanup function responsible for releasing all resources associated with an LLVM JIT compilation context when it's no longer needed.

## Definition


## Detailed Description
This function performs comprehensive cleanup of an LLVM JIT context, including disposing of LLVM modules, cleaning up JIT handles, and managing resource tracking. It implements version-specific cleanup logic for different LLVM versions (>11 vs ≤11) and includes safety measures to avoid cleanup during process exit to prevent potential reentrancy issues with LLVM's error handling.

The function decrements the global usage counter and handles both the LLVM-specific cleanup (modules, execution sessions, symbol pools) and PostgreSQL-specific cleanup (resource owner tracking).

## Parameters / Member Variables
- : JitContext pointer that gets cast to LLVMJitContext for LLVM-specific cleanup operations

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