# FunctionReturningBool

## Location
[src/backend/jit/llvm/llvmjit_types.c:128-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit_types.c#L128-L185)

## Overview
FunctionReturningBool is a template function used by PostgreSQL's LLVM JIT compiler to determine the correct representation width for boolean return values across different architectures and compilers.

## Definition
```c
bool FunctionReturningBool(void)
```

## Detailed Description
FunctionReturningBool serves a specific purpose in PostgreSQL's LLVM JIT compilation system to handle cross-architecture and cross-compiler compatibility for boolean types. The function addresses a particular issue where Clang represents stdbool.h style booleans differently when they are returned by functions (as i1 integers) versus when they are stored (as i8 integers). By providing this concrete template function, the JIT system can determine the correct width and representation of returned boolean values, ensuring compatibility with both stdbool-using and non-stdbool architectures. The function itself simply returns false and serves purely as a type template for the LLVM type system analysis.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - None (returns constant false)
- Called from (representative examples):
  - [ExecEvalBoolSubroutineTemplate](../E/ExecEvalBoolSubroutineTemplate.md) (referenced in llvmjit_types.c:126)

## Notes and Other Information
- This function is part of the JIT template system and should not be called directly during normal PostgreSQL operations
- Specifically designed to solve the boolean representation problem in LLVM where return types and storage types have different widths
- The comment above the function explains that Clang represents returned booleans as i1 but stored booleans as i8
- Critical for cross-platform compatibility, especially for architectures that do not use stdbool.h
- Part of the type template infrastructure that allows the JIT compiler to generate correctly-typed functions for different target architectures
- The function is deliberately simple, containing only a return statement, to serve as a clean type reference