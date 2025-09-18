# LLVMJitContext

## Location
src/include/jit/llvmjit.h: 43 - 71

## Overview
LLVMJitContext is a structure that extends the base JitContext to provide LLVM-specific just-in-time compilation functionality in PostgreSQL. It manages LLVM compilation state, module generation, and resource tracking for efficient code generation and execution.

## Definition


## Detailed Description
LLVMJitContext is the main structure that manages LLVM-based just-in-time compilation in PostgreSQL. It extends the base JitContext with LLVM-specific functionality and state management. This structure coordinates LLVM compilation processes, manages memory resources, tracks compilation state, and maintains references to compiled code objects.

The context manages the lifecycle of LLVM modules and compilation units, ensuring proper resource cleanup and providing mechanisms for code generation, optimization, and execution. It serves as the central coordination point for all LLVM JIT operations within PostgreSQL's query execution pipeline.

## Parameters / Member Variables
- : Base JitContext structure containing common JIT flags and instrumentation data
- : ResourceOwner used to ensure proper cleanup of the JIT context and associated resources
- : Counter tracking the number of modules created, used for versioning and management
- : Reference to the LLVM context used for compilation; reused across compilations but occasionally reset to prevent excessive memory usage
- : Reference to the current LLVM module that is "open for write" and accepting new code
- : Boolean flag indicating whether there is pending code that needs to be emitted
- : Counter for the number of objects emitted, used to generate non-conflicting symbol names
- : List of handles for code emitted via LLVM's ORC (On Request Compilation) JIT infrastructure

## Dependencies
- Functions called/Symbols referenced:
  - [JitContext](../J/JitContext.md)
  - ResourceOwner
  - LLVMContextRef (LLVM API)
  - LLVMModuleRef (LLVM API)
  - [List](List.md) (PostgreSQL list structure)

- Called from (representative examples):
  - [llvm_create_context](../l/llvm_create_context.md)
  - [llvm_release_context](../l/llvm_release_context.md)
  - [llvm_mutable_module](../l/llvm_mutable_module.md)
  - llvm_compile_expr
  - [slot_compile_deform](../s/slot_compile_deform.md)
  - [llvm_compile_module](../l/llvm_compile_module.md)
  - [llvm_optimize_module](../l/llvm_optimize_module.md)
  - ResOwnerReleaseJitContext

## Notes and Other Information
- The LLVM context is periodically reset to prevent excessive memory accumulation from type information
- The structure is designed to support resource management through PostgreSQL's ResourceOwner system
- The ORC handles list manages dynamically compiled code objects for efficient execution
- Module generation tracking helps with debugging and performance analysis
- The compiled flag helps optimize when code emission is actually necessary
- This structure is central to PostgreSQL's LLVM-based expression compilation and tuple deforming optimizations