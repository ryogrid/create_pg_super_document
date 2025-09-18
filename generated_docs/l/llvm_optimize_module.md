# llvm_optimize_module

## Location
src/backend/jit/llvm/llvmjit.c: 636 - 732

## Overview
Optimizes LLVM IR code in a module using the optimization flags set in the JIT context, applying different optimization passes based on the configured optimization level.

## Definition


## Detailed Description
This function performs code optimization on an LLVM module using either the legacy pass manager (LLVM < 17) or the new pass manager (LLVM >= 17). The optimization strategy is determined by the flags set in the JIT context:

**Legacy Pass Manager (LLVM < 17):**
- Creates a new PassManagerBuilder for each optimization run to avoid state issues with the inliner
- Applies function-level optimizations first, then module-level optimizations
- Uses different optimization levels (O0 or O3) based on PGJIT_OPT3 flag
- Always includes mem2reg pass even at O0 level due to heavy reliance on it
- Supports inlining with configurable thresholds

**New Pass Manager (LLVM >= 17):**
- Uses string-based pass specifications ("default<O3>" or "default<O0>,mem2reg")
- Simplified interface with PassBuilderOptions
- Maintains similar optimization philosophy but with modern LLVM infrastructure

## Parameters / Member Variables
- : LLVMJitContext pointer containing JIT compilation flags and module reference
- : LLVMModuleRef representing the LLVM module to be optimized

## Dependencies
- Functions called/Symbols referenced:
  - [LLVMJitContext](../L/LLVMJitContext.md) (context structure)
  - PGJIT_OPT3 (optimization flag)
  - PGJIT_INLINE (inlining flag)
  - llvm_error_message (error handling function)
- Called from (representative examples):
  - [llvm_compile_module](llvm_compile_module.md)

## Notes and Other Information
- The function handles version differences between LLVM < 17 and >= 17 with conditional compilation
- Inlining threshold is set to 512 (noted as "unscientifically determined")
- mem2reg pass is always applied even at O0 optimization level due to PostgreSQL's heavy reliance on it
- Function-level optimization is performed before module-level optimization
- Pass managers are properly disposed of to prevent memory leaks
- Error handling is implemented for the new pass manager with descriptive error messages