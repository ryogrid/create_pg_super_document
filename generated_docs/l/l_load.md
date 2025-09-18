# l_load

## Location
src/include/jit/llvmjit_emit.h: 128 - 137

## Overview
A static inline function that provides a version-agnostic wrapper for LLVM's load instruction, ensuring compatibility across different LLVM versions.

## Definition
```c
static inline LLVMValueRef l_load(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, const char *name)
```

## Detailed Description
The `l_load` function is a compatibility wrapper for LLVM's load instruction builder. It automatically selects the appropriate LLVM API function based on the LLVM version being used. For LLVM versions prior to 16, it uses `LLVMBuildLoad`, while for version 16 and later, it uses `LLVMBuildLoad2`. This abstraction allows PostgreSQL's JIT compilation code to work seamlessly across different LLVM versions without requiring conditional compilation throughout the codebase.

The load instruction is one of the most fundamental operations in LLVM, used to read values from memory locations pointed to by pointers. It's essential for accessing variables, struct members, and array elements in compiled code.

## Parameters / Member Variables
- `b`: LLVM builder reference used to insert the instruction
- `t`: LLVM type reference specifying the type to load (used in LLVM 16+)
- `v`: LLVM value reference representing the pointer to load from
- `name`: String name for the resulting LLVM value (for debugging/readability)

## Dependencies
- Functions called/Symbols referenced:
  - LLVMBuildLoad (LLVM < 16)
  - LLVMBuildLoad2 (LLVM >= 16)
- Called from (representative examples):
  - [llvm_function_reference](llvm_function_reference.md) (in llvmjit.c)
  - [slot_compile_deform](../s/slot_compile_deform.md) (extensively in llvmjit_deform.c)
  - llvm_compile_expr (extensively in llvmjit_expr.c)
  - BuildV1Call
  - [l_load_struct_gep](l_load_struct_gep.md)
  - [l_load_gep1](l_load_gep1.md)
  - [l_mcxt_switch](l_mcxt_switch.md)
  - [l_funcnull](l_funcnull.md)
  - [l_funcvalue](l_funcvalue.md)

## Notes and Other Information
- This function is part of PostgreSQL's JIT compilation infrastructure
- Extremely widely used throughout the JIT compilation system - one of the most frequently called utility functions
- The conditional compilation ensures compatibility across LLVM version boundaries where API changes occurred
- Essential for reading values from memory in generated LLVM code, making it a core building block for JIT operations
- Used extensively in tuple slot operations, expression evaluation, and function calls within the JIT compiler