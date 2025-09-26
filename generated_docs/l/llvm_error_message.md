# llvm_error_message

## Location
src/backend/jit/llvm/llvmjit.c: 1364 - 1379

## Overview
A static utility function that converts LLVM error references into PostgreSQL-managed string copies for safe error reporting.

## Definition
```c
static char *llvm_error_message(LLVMErrorRef error)
```

## Detailed Description
This function serves as a bridge between LLVM's error handling system and PostgreSQL's memory management. It takes an LLVM error reference and converts it to a PostgreSQL-allocated string that can be safely used throughout the PostgreSQL codebase.

The function performs three key operations:
1. Extracts the error message string from the LLVM error reference using LLVMGetErrorMessage
2. Creates a PostgreSQL-managed copy of the string using pstrdup, which allocates memory in the current PostgreSQL memory context
3. Properly disposes of the LLVM-allocated original string to prevent memory leaks

This conversion is essential because LLVM and PostgreSQL have different memory management systems. LLVM-allocated strings must be freed using LLVM's disposal functions, while PostgreSQL expects strings to be allocated within its memory context system for automatic cleanup.

## Parameters / Member Variables
- `error`: LLVM error reference containing error information to be converted to a string

## Dependencies
- Functions called/Symbols referenced:
  - LLVMGetErrorMessage (LLVM API function)
  - pstrdup (PostgreSQL string duplication function)
  - LLVMDisposeErrorMessage (LLVM memory cleanup function)
- Called from (representative examples):
  - llvm_get_function (for JIT lookup errors)
  - llvm_optimize_module (for optimization errors)
  - llvm_compile_module (for compilation errors)
  - llvm_log_jit_error (for general JIT errors)
  - llvm_create_jit_instance (for JIT instance creation errors)

## Notes and Other Information
- This is a static function local to llvmjit.c
- Essential for proper memory management between LLVM and PostgreSQL systems
- Prevents memory leaks by properly disposing of LLVM-allocated strings
- The returned string is allocated in PostgreSQL's current memory context
- Used throughout the JIT infrastructure for consistent error message handling
- Critical for converting LLVM errors into PostgreSQL-compatible format for logging and error reporting