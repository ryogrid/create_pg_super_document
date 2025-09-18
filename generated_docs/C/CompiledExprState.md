# CompiledExprState

## Location
[src/backend/jit/llvm/llvmjit_expr.c:48-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit_expr.c#L48-L52)

## Overview
A private state structure used to store JIT compilation context and function name for compiled expressions in PostgreSQL's LLVM-based expression evaluation system.

## Definition


## Detailed Description
 serves as a bridge between the high-level expression state () and the low-level LLVM JIT compilation infrastructure. This structure is stored in the  field of  when an expression has been successfully compiled with LLVM JIT.

The structure maintains two critical pieces of information needed to execute a JIT-compiled expression: a reference to the LLVM JIT context that manages the compilation environment, and the generated function name that can be used to retrieve the compiled function from the LLVM execution engine.

This design allows PostgreSQL to efficiently manage JIT-compiled expressions by providing a lightweight state object that connects the expression evaluation framework with the underlying LLVM compilation infrastructure.

## Parameters / Member Variables
- : Pointer to the  that contains the LLVM compilation environment, including the LLVM context, module, and execution handles. This context manages the lifetime and compilation of the JIT-generated code.
- : String containing the name of the generated LLVM function that implements the compiled expression. This name is used to retrieve the function pointer from the LLVM execution engine when the expression needs to be executed.

## Dependencies
- Functions called/Symbols referenced:
  - [LLVMJitContext](../L/LLVMJitContext.md)
- Called from (representative examples):
  - llvm_compile_expr
  - ExecRunCompiledExpr

## Notes and Other Information
- This structure is allocated and populated during the  process when an expression is successfully JIT-compiled
- The structure is used by  as a transition mechanism - it's only used for the first execution of a compiled expression, after which the direct function pointer replaces the indirection
- The  is generated using  with the base name "evalexpr" during compilation
- This is part of PostgreSQL's JIT infrastructure introduced to accelerate expression evaluation in performance-critical query execution paths
- The structure provides a clean separation between PostgreSQL's expression evaluation framework and the LLVM compilation details