# llvm_get_function

## Location
[src/backend/jit/llvm/llvmjit.c:381-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L381-L454)

## Overview
Retrieves a compiled function pointer by name from the LLVM JIT context, triggering compilation if needed and handling version-specific LLVM symbol lookup.

## Definition

```c
void *
llvm_get_function(LLVMJitContext *context, const char *funcname)
```
## Detailed Description
This function provides access to JIT-compiled functions by looking up their symbols in the LLVM execution engine. It handles the complete compilation pipeline - if code is pending compilation, it triggers compilation first via llvm_compile_module(). The function implements version-specific symbol lookup using different LLVM ORC APIs depending on the LLVM version (>11 uses LLJIT, ≤11 uses OrcStack). For newer LLVM versions, it also tracks emission timing since LLJIT performs lazy code generation on first symbol access.

## Parameters / Member Variables
- `*context`: LLVMJitContext pointer containing compiled modules and JIT handles
- `*funcname`: Name of the function to look up (unmangled symbol name)
## Dependencies
- Functions called/Symbols referenced:
  - llvm_assert_in_fatal_section (safety assertion)
  - [llvm_compile_module](llvm_compile_module.md) (triggers compilation if needed)
  - LLVMOrcLLJITLookup (LLVM 12+ symbol lookup)
  - LLVMOrcGetSymbolAddressIn (LLVM ≤11 symbol lookup)
  - [llvm_error_message](llvm_error_message.md) (error handling utility)
  - INSTR_TIME_SET_CURRENT/INSTR_TIME_ACCUM_DIFF (timing instrumentation)
  - elog (PostgreSQL error reporting)
- Called from:
  - [ExecRunCompiledExpr](../E/ExecRunCompiledExpr.md) (expression execution)

## Notes and Other Information
- Requires being called within a fatal section for safety
- Uses unmangled symbol names since ORC handles unmangled symbols
- Implements lazy compilation - compiles pending modules on first access
- Version-dependent implementation for LLVM compatibility
- Includes timing instrumentation for emission tracking in LLVM 12+
- Returns NULL and throws ERROR if function cannot be found
- In LLVM 12+, first symbol lookup triggers actual code emission (lazy evaluation)

## Simplified Source

```c
void *
llvm_get_function(LLVMJitContext *context, const char *funcname)
{
    llvm_assert_in_fatal_section();

    // Compile pending modules if needed
    if (!context->compiled) {
        llvm_compile_module(context);
    }

    // Search through all JIT handles for the function
    foreach(lc, context->handles) {
        LLVMJitHandle *handle = (LLVMJitHandle *) lfirst(lc);

#if LLVM_VERSION_MAJOR > 11
        // LLVM 12+ using LLJIT
        LLVMOrcJITTargetAddress addr = 0;
        LLVMErrorRef error = LLVMOrcLLJITLookup(handle->lljit, &addr, funcname);
        if (error) {
            elog(ERROR, "failed to look up symbol \"%s\": %s",
                 funcname, llvm_error_message(error));
        }

        // Track emission timing for lazy compilation
        INSTR_TIME_ACCUM_DIFF(context->base.instr.emission_counter, endtime, starttime);

        if (addr)
            return (void *) (uintptr_t) addr;
#else
        // LLVM 11 and older using OrcStack
        LLVMOrcTargetAddress addr = 0;
        if (LLVMOrcGetSymbolAddressIn(handle->stack, &addr, handle->orc_handle, funcname)) {
            elog(ERROR, "failed to look up symbol \"%s\"", funcname);
        }
        if (addr)
            return (void *) (uintptr_t) addr;
#endif
    }

    elog(ERROR, "failed to JIT: %s", funcname);
    return NULL;
}
```