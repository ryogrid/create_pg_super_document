# llvm_create_types

## Location
[src/backend/jit/llvm/llvmjit.c:1091-1144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1091-L1144)

## Overview
Loads and initializes PostgreSQL data structure type definitions and function templates from a precompiled LLVM bitcode module for use in JIT compilation.

## Definition

```c
static void
llvm_create_types(void)
```
## Detailed Description
This function is responsible for loading the essential LLVM type definitions and function templates that the PostgreSQL JIT compilation system needs. It reads from the precompiled bitcode file  located in the PostgreSQL library directory and extracts LLVM type representations for various PostgreSQL data structures and function signatures.

The function performs several key operations:
1. Constructs the path to the  file
2. Loads the bitcode file into memory using LLVM's memory buffer API
3. Parses the bitcode into an LLVM module ()
4. Extracts type definitions for PostgreSQL structures using 
5. Loads function return types using 
6. Retrieves function templates for code generation

This initialization ensures that JIT-compiled code can properly interface with PostgreSQL's internal data structures and follows the correct calling conventions.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library)
  -  (LLVM API)
  -  (LLVM API)  
  -  (LLVM API)
  -  (PostgreSQL LLVM utility)
  -  (PostgreSQL LLVM utility)
  -  (LLVM API)
  -  (PostgreSQL error logging)
- Global variables accessed:
  -  (PostgreSQL library path)
  -  (global LLVM context)
  -  (global types module)
  - Various type globals (TypeSizeT, TypeParamBool, etc.)
  - Function template globals (AttributeTemplate, etc.)
- Called from:
  -  at src/backend/jit/llvm/llvmjit.c:224
  -  at src/backend/jit/llvm/llvmjit.c:907

## Notes and Other Information
- The function loads 21 different PostgreSQL struct type definitions including core types like ExprContext, ExprState, TupleTableSlot, and aggregation-related structures
- Three function templates are loaded for different evaluation scenarios (attribute access, general subroutines, boolean subroutines)
- The bitcode file must exist and be readable, otherwise the function will error out
- This function is called during LLVM context initialization and recreation
- The loaded types are used throughout the JIT compilation process to ensure type safety and proper memory layout

## Simplified Source

```c
static void
llvm_create_types(void)
{
    char path[MAXPGPATH];
    LLVMMemoryBufferRef buf;
    char *msg;

    // Build path to types bitcode file
    snprintf(path, MAXPGPATH, "%s/%s", pkglib_path, "llvmjit_types.bc");

    // Load bitcode file into memory
    if (LLVMCreateMemoryBufferWithContentsOfFile(path, &buf, &msg)) {
        elog(ERROR, "LLVMCreateMemoryBufferWithContentsOfFile(%s) failed: %s", path, msg);
    }

    // Parse bitcode into LLVM module
    if (LLVMParseBitcodeInContext2(llvm_context, buf, &llvm_types_module)) {
        elog(ERROR, "LLVMParseBitcodeInContext2 of %s failed", path);
    }
    LLVMDisposeMemoryBuffer(buf);

    // Load PostgreSQL type definitions
    TypeSizeT = llvm_pg_var_type("TypeSizeT");
    TypeParamBool = load_return_type(llvm_types_module, "FunctionReturningBool");
    TypeStorageBool = llvm_pg_var_type("TypeStorageBool");
    StructExprContext = llvm_pg_var_type("StructExprContext");
    StructExprState = llvm_pg_var_type("StructExprState");
    StructTupleTableSlot = llvm_pg_var_type("StructTupleTableSlot");
    // ... additional type definitions

    // Load function templates
    AttributeTemplate = LLVMGetNamedFunction(llvm_types_module, "AttributeTemplate");
    ExecEvalSubroutineTemplate = LLVMGetNamedFunction(llvm_types_module, "ExecEvalSubroutineTemplate");
    ExecEvalBoolSubroutineTemplate = LLVMGetNamedFunction(llvm_types_module, "ExecEvalBoolSubroutineTemplate");
}
```