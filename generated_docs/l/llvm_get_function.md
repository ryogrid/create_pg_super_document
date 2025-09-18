# llvm_get_function

## Location
src/backend/jit/llvm/llvmjit.c: 381 - 454

## Overview
Retrieves a compiled function pointer by name from the LLVM JIT context, triggering compilation if needed and handling version-specific LLVM symbol lookup.

## Definition


## Detailed Description
This function provides access to JIT-compiled functions by looking up their symbols in the LLVM execution engine. It handles the complete compilation pipeline - if code is pending compilation, it triggers compilation first via llvm_compile_module(). The function implements version-specific symbol lookup using different LLVM ORC APIs depending on the LLVM version (>11 uses LLJIT, ≤11 uses OrcStack). For newer LLVM versions, it also tracks emission timing since LLJIT performs lazy code generation on first symbol access.

## Parameters / Member Variables
- : LLVMJitContext pointer containing compiled modules and JIT handles
- : Name of the function to look up (unmangled symbol name)

## Dependencies
- Functions called/Symbols referenced:
  - llvm_assert_in_fatal_section (safety assertion)
  - llvm_compile_module (triggers compilation if needed)
  - LLVMOrcLLJITLookup (LLVM 12+ symbol lookup)
  - LLVMOrcGetSymbolAddressIn (LLVM ≤11 symbol lookup)
  - llvm_error_message (error handling utility)
  - INSTR_TIME_SET_CURRENT/INSTR_TIME_ACCUM_DIFF (timing instrumentation)
  - elog (PostgreSQL error reporting)
- Called from:
  - ExecRunCompiledExpr (expression execution)

## Notes and Other Information
- Requires being called within a fatal section for safety
- Uses unmangled symbol names since ORC handles unmangled symbols
- Implements lazy compilation - compiles pending modules on first access
- Version-dependent implementation for LLVM compatibility
- Includes timing instrumentation for emission tracking in LLVM 12+
- Returns NULL and throws ERROR if function cannot be found
- In LLVM 12+, first symbol lookup triggers actual code emission (lazy evaluation)