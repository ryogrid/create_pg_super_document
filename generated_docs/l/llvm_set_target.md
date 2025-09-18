# llvm_set_target

## Location
[src/backend/jit/llvm/llvmjit.c:1072-1090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1072-L1090)

## Overview
Initializes the LLVM target architecture and data layout information by extracting configuration from the pre-compiled types module to ensure compatibility.

## Definition


## Detailed Description
This static function serves as an initialization routine for the LLVM JIT compilation system in PostgreSQL. It extracts and caches the target triple and data layout string from the , which contains pre-compiled type definitions. The function ensures that the LLVM JIT compiler uses the same target architecture and memory layout that was used when the types module was compiled, guaranteeing compatibility between the JIT-compiled code and the rest of the PostgreSQL system.

The function performs lazy initialization - it only sets the global variables  and  if they haven't been set previously. This approach allows the target information to be determined once and reused throughout the session.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL error logging)
  -  (PostgreSQL string duplication)
  -  (LLVM API function)
  -  (LLVM API function)
- Global variables accessed:
  -  (LLVM module containing type definitions)
  -  (global variable for target triple)
  -  (global variable for data layout)
- Called from:
  -  at src/backend/jit/llvm/llvmjit.c:912

## Notes and Other Information
- This function must be called after  has been loaded, otherwise it will throw an ERROR
- The function uses PostgreSQL's memory management () to ensure proper cleanup
- The target triple and data layout are extracted from a pre-compiled module to guarantee ABI compatibility
- This is part of PostgreSQL's LLVM JIT compilation infrastructure introduced for query optimization