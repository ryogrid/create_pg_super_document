# llvm_compile_module

## Location
[src/backend/jit/llvm/llvmjit.c:733-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L733-L863)

## Overview
Compiles the currently pending LLVM module by performing inlining, optimization, and code emission, creating a handle for runtime function lookups.

## Definition

```c
struction combining/selection passes etc. Without optimization a
	 * faster instruction selection mechanism is used.
	 */
	INSTR_TIME_SET_CURRENT(starttime);
```
## Detailed Description
This function orchestrates the complete compilation pipeline for an LLVM module:

1. **ORC JIT Selection**: Chooses between optimized (llvm_opt3_orc) and unoptimized (llvm_opt0_orc) JIT compilers based on PGJIT_OPT3 flag
2. **Inlining Phase**: Optionally performs function inlining if PGJIT_INLINE flag is set, with timing instrumentation
3. **Bitcode Dumping**: Optionally dumps bitcode files for debugging (controlled by jit_dump_bitcode)
4. **Optimization**: Calls llvm_optimize_module to apply optimization passes with timing measurement
5. **Code Emission**: Handles module emission differently based on LLVM version:
   - **LLVM > 11**: Uses ThreadSafeModule and lazy compilation via LLVMOrcLLJITAddLLVMIRModuleWithRT
   - **LLVM ≤ 11**: Uses eager compilation via LLVMOrcAddEagerlyCompiledIR
6. **Handle Management**: Creates and tracks compilation handles for cleanup and symbol lookup

The function includes comprehensive timing instrumentation for performance analysis and debugging output.

## Parameters / Member Variables
- : LLVMJitContext pointer containing the module to compile, optimization flags, and instrumentation counters

## Dependencies
- Functions called/Symbols referenced:
  - [LLVMJitHandle](../L/LLVMJitHandle.md) (handle structure)
  - PGJIT_OPT3 (optimization level flag)
  - PGJIT_INLINE (inlining flag)
  - [llvm_optimize_module](llvm_optimize_module.md) (optimization function)
  - [llvm_error_message](llvm_error_message.md) (error handling)
  - [llvm_resolve_symbol](llvm_resolve_symbol.md) (symbol resolution)
  - INSTR_TIME_* macros (timing instrumentation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (memory management)
- Called from (representative examples):
  - [llvm_get_function](llvm_get_function.md)

## Notes and Other Information
- The function transfers ownership of the LLVM module to the ORC JIT compiler
- For LLVM > 11, code emission is lazy - actual compilation happens when symbols are first requested
- For LLVM ≤ 11, compilation is eager - all code is compiled immediately
- Timing information is collected for three phases: inlining, optimization, and emission
- Handles are stored in TopMemoryContext for persistence across PostgreSQL memory context switches
- Debug output provides detailed timing breakdown for performance analysis
- Bitcode files can be dumped both before and after optimization for debugging purposes
- The function sets context->compiled = true and context->module = NULL after successful compilation

## Simplified Source

```c
static void
llvm_compile_module(LLVMJitContext *context)
{
    LLVMJitHandle *handle;
    MemoryContext oldcontext;

    // Select optimization level
    compile_orc = (context->base.flags & PGJIT_OPT3) ? llvm_opt3_orc : llvm_opt0_orc;

    // Perform inlining if requested
    if (context->base.flags & PGJIT_INLINE) {
        llvm_inline(context->module);
    }

    // Optimize the module
    llvm_optimize_module(context, context->module);

    // Create handle for compiled code
    handle = MemoryContextAlloc(TopMemoryContext, sizeof(LLVMJitHandle));

    // Emit code (version-specific logic)
    if (LLVM_VERSION_MAJOR > 11) {
        // Use new ORC API with lazy compilation
        LLVMOrcThreadSafeModuleRef ts_module =
            LLVMOrcCreateNewThreadSafeModule(context->module, llvm_ts_context);
        handle->lljit = compile_orc;
        LLVMOrcLLJITAddLLVMIRModuleWithRT(compile_orc, handle->resource_tracker, ts_module);
    } else {
        // Use legacy eager compilation
        LLVMOrcAddEagerlyCompiledIR(compile_orc, &handle->orc_handle,
                                   context->module, llvm_resolve_symbol, NULL);
    }

    // Update context state
    context->module = NULL;
    context->compiled = true;

    // Track handle for cleanup
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    context->handles = lappend(context->handles, handle);
    MemoryContextSwitchTo(oldcontext);
}
```