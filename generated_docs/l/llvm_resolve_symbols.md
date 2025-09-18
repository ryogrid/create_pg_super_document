# llvm_resolve_symbols

## Location
[src/backend/jit/llvm/llvmjit.c:1220-1264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1220-L1264)

## Overview
LLVM ORC JIT callback function that resolves multiple undefined symbols simultaneously by creating absolute symbol definitions for the JIT execution engine.

## Definition


## Detailed Description
This function serves as a callback for LLVM's ORC (On-Request Compilation) JIT system when it needs to resolve multiple undefined symbols. It's part of LLVM's lazy compilation and symbol resolution mechanism, which allows the JIT to resolve symbols on-demand as they are needed.

The function operates by:
1. Allocating memory for a symbol map array based on the lookup set size
2. Iterating through each symbol in the lookup set
3. Extracting the symbol name and calling  to get its address
4. Creating symbol map entries with the resolved addresses and appropriate flags
5. Creating an absolute symbols materialization unit and defining it in the JIT dylib
6. Properly handling LLVM version differences in data structures and API calls

The function includes version-specific handling for LLVM API changes, particularly between LLVM versions 12, 14, and later versions.

## Parameters / Member Variables
- : Definition generator reference (unused)
- : Context parameter (unused)
- : Lookup state reference (unused)
- : Lookup kind specification (unused)
- : JIT dynamic library reference where symbols will be defined
- : Flags for JIT dylib lookup (unused)
- : Array of symbols that need to be resolved
- : Number of symbols in the lookup set
- Returns: LLVM error reference (LLVMErrorSuccess on success)

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL zero-initialized memory allocation)
  -  (LLVM API)
  -  (LLVM API, version > 12)
  -  (PostgreSQL LLVM symbol resolver)
  -  (LLVM API)
  -  (LLVM API)
  -  (LLVM API)
  -  (PostgreSQL memory deallocation)
- Version-dependent types:
  -  (LLVM > 14)
  -  (LLVM ≤ 14)
- Called from:
  -  at src/backend/jit/llvm/llvmjit.c:1354
  -  at src/backend/jit/llvm/llvmjit.c:1356

## Notes and Other Information
- This function handles batch symbol resolution, which is more efficient than resolving symbols one at a time
- It includes version-specific conditional compilation for different LLVM API versions
- The function sets the  flag for all resolved symbols
- Memory management is handled through PostgreSQL's allocation functions rather than standard C library functions
- Error handling follows LLVM's error model, returning error references that can be checked by the caller
- This is a critical component for PostgreSQL's JIT compilation system, enabling dynamic symbol resolution during code generation