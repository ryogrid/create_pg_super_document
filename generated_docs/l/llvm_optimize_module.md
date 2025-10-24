# llvm_optimize_module

## Location
[src/backend/jit/llvm/llvmjit.c:636-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L636-L732)

## Overview
Optimizes LLVM IR code in a module using the optimization flags set in the JIT context, applying different optimization passes based on the configured optimization level.

## Definition

```c
static void
llvm_optimize_module(LLVMJitContext *context, LLVMModuleRef module)
```
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
- `*context`: LLVMJitContext pointer containing JIT compilation flags and module reference
- `module`: LLVMModuleRef representing the LLVM module to be optimized
## Dependencies
- Functions called/Symbols referenced:
  - [LLVMJitContext](../L/LLVMJitContext.md) (context structure)
  - PGJIT_OPT3 (optimization flag)
  - PGJIT_INLINE (inlining flag)
  - [llvm_error_message](llvm_error_message.md) (error handling function)
- Called from (representative examples):
  - [llvm_compile_module](llvm_compile_module.md)

## Notes and Other Information
- The function handles version differences between LLVM < 17 and >= 17 with conditional compilation
- Inlining threshold is set to 512 (noted as "unscientifically determined")
- mem2reg pass is always applied even at O0 optimization level due to PostgreSQL's heavy reliance on it
- Function-level optimization is performed before module-level optimization
- Pass managers are properly disposed of to prevent memory leaks
- Error handling is implemented for the new pass manager with descriptive error messages

## Simplified Source

```c
static void
llvm_optimize_module(LLVMJitContext *context, LLVMModuleRef module)
{
#if LLVM_VERSION_MAJOR < 17
    // Legacy pass manager for LLVM < 17
    LLVMPassManagerBuilderRef llvm_pmb;
    LLVMPassManagerRef llvm_mpm, llvm_fpm;

    // Determine optimization level
    int compile_optlevel = (context->base.flags & PGJIT_OPT3) ? 3 : 0;

    // Create new pass manager builder (required for inliner state)
    llvm_pmb = LLVMPassManagerBuilderCreate();
    LLVMPassManagerBuilderSetOptLevel(llvm_pmb, compile_optlevel);
    llvm_fpm = LLVMCreateFunctionPassManagerForModule(module);

    // Configure inlining and optimization passes
    if (context->base.flags & PGJIT_OPT3) {
        LLVMPassManagerBuilderUseInlinerWithThreshold(llvm_pmb, 512);
    } else {
        // Always include mem2reg pass even at O0
        LLVMAddPromoteMemoryToRegisterPass(llvm_fpm);
    }

    LLVMPassManagerBuilderPopulateFunctionPassManager(llvm_pmb, llvm_fpm);

    // Run function-level optimization
    LLVMInitializeFunctionPassManager(llvm_fpm);
    for (LLVMValueRef func = LLVMGetFirstFunction(context->module);
         func != NULL;
         func = LLVMGetNextFunction(func)) {
        LLVMRunFunctionPassManager(llvm_fpm, func);
    }
    LLVMFinalizeFunctionPassManager(llvm_fpm);
    LLVMDisposePassManager(llvm_fpm);

    // Run module-level optimization
    llvm_mpm = LLVMCreatePassManager();
    LLVMPassManagerBuilderPopulateModulePassManager(llvm_pmb, llvm_mpm);

    // Add additional passes based on flags
    if (!(context->base.flags & PGJIT_OPT3)) {
        LLVMAddAlwaysInlinerPass(llvm_mpm);
    }
    if ((context->base.flags & PGJIT_INLINE) && !(context->base.flags & PGJIT_OPT3)) {
        LLVMAddFunctionInliningPass(llvm_mpm);
    }

    LLVMRunPassManager(llvm_mpm, context->module);
    LLVMDisposePassManager(llvm_mpm);
    LLVMPassManagerBuilderDispose(llvm_pmb);

#else
    // New pass manager for LLVM >= 17
    const char *passes = (context->base.flags & PGJIT_OPT3) ?
                         "default<O3>" : "default<O0>,mem2reg";

    LLVMPassBuilderOptionsRef options = LLVMCreatePassBuilderOptions();
    LLVMPassBuilderOptionsSetInlinerThreshold(options, 512);

    LLVMErrorRef err = LLVMRunPasses(module, passes, NULL, options);
    if (err) {
        elog(ERROR, "failed to JIT module: %s", llvm_error_message(err));
    }

    LLVMDisposePassBuilderOptions(options);
#endif
}
```