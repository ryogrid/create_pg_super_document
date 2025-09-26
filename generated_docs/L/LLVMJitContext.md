# LLVMJitContext

## Location
[src/include/jit/llvmjit.h:43-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit.h#L43-L71)

## Overview
LLVMJitContext is a structure that extends the base JitContext to provide LLVM-specific just-in-time compilation functionality in PostgreSQL. It manages LLVM compilation state, module generation, and resource tracking for efficient code generation and execution.

## Definition

```c
typedef struct LLVMJitContext
{
	JitContext	base;

	/* used to ensure cleanup of context */
	ResourceOwner resowner;

	/* number of modules created */
	size_t		module_generation;

	/*
	 * The LLVM Context used by this JIT context. An LLVM context is reused
	 * across many compilations, but occasionally reset to prevent it using
	 * too much memory due to more and more types accumulating.
	 */
	LLVMContextRef llvm_context;

	/* current, "open for write", module */
	LLVMModuleRef module;

	/* is there any pending code that needs to be emitted */
	bool		compiled;

	/* # of objects emitted, used to generate non-conflicting names */
	int			counter;

	/* list of handles for code emitted via Orc */
	List	   *handles;
} LLVMJitContext;
```
## Detailed Description
LLVMJitContext is the main structure that manages LLVM-based just-in-time compilation in PostgreSQL. It extends the base JitContext with LLVM-specific functionality and state management. This structure coordinates LLVM compilation processes, manages memory resources, tracks compilation state, and maintains references to compiled code objects.

The context manages the lifecycle of LLVM modules and compilation units, ensuring proper resource cleanup and providing mechanisms for code generation, optimization, and execution. It serves as the central coordination point for all LLVM JIT operations within PostgreSQL's query execution pipeline.

## Parameters / Member Variables
- `base`: Base JitContext structure containing common JIT flags and instrumentation data
- `resowner`: ResourceOwner used to ensure proper cleanup of the JIT context and associated resources
- `module_generation`: Counter tracking the number of modules created, used for versioning and management
- `llvm_context`: Reference to the LLVM context used for compilation; reused across compilations but occasionally reset to prevent excessive memory usage
- `module`: Reference to the current LLVM module that is "open for write" and accepting new code
- `compiled`: Boolean flag indicating whether there is pending code that needs to be emitted
- `counter`: Counter for the number of objects emitted, used to generate non-conflicting symbol names
- `*handles`: List of handles for code emitted via LLVM's ORC (On Request Compilation) JIT infrastructure
## Dependencies
- Functions called/Symbols referenced:
  - [JitContext](../J/JitContext.md)
  - [ResourceOwner](../R/ResourceOwner.md)
  - LLVMContextRef (LLVM API)
  - LLVMModuleRef (LLVM API)
  - [List](List.md) (PostgreSQL list structure)

- Called from (representative examples):
  - [llvm_create_context](../l/llvm_create_context.md)
  - [llvm_release_context](../l/llvm_release_context.md)
  - [llvm_mutable_module](../l/llvm_mutable_module.md)
  - [llvm_compile_expr](../l/llvm_compile_expr.md)
  - [slot_compile_deform](../s/slot_compile_deform.md)
  - [llvm_compile_module](../l/llvm_compile_module.md)
  - [llvm_optimize_module](../l/llvm_optimize_module.md)
  - [ResOwnerReleaseJitContext](../R/ResOwnerReleaseJitContext.md)

## Notes and Other Information
- The LLVM context is periodically reset to prevent excessive memory accumulation from type information
- The structure is designed to support resource management through PostgreSQL's ResourceOwner system
- The ORC handles list manages dynamically compiled code objects for efficient execution
- Module generation tracking helps with debugging and performance analysis
- The compiled flag helps optimize when code emission is actually necessary
- This structure is central to PostgreSQL's LLVM-based expression compilation and tuple deforming optimizations