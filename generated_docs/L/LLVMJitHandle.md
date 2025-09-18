# LLVMJitHandle

## Location
src/backend/jit/llvm/llvmjit.c: 52 - 61

## Overview
LLVMJitHandle is a structure that represents a compiled module handle emitted via the LLVM ORC JIT (On Request Compilation) system in PostgreSQL's JIT compilation infrastructure.

## Definition


## Detailed Description
LLVMJitHandle serves as a version-agnostic wrapper for managing compiled LLVM modules within PostgreSQL's JIT compilation system. The structure contains different member variables depending on the LLVM version being used, reflecting the evolution of LLVM's ORC JIT API between major versions.

For LLVM versions greater than 11, it uses the newer LLJIT (Lazy LLVM JIT) interface with resource tracking capabilities. For older versions (11 and below), it uses the legacy ORC JIT stack interface. This design allows PostgreSQL to support multiple LLVM versions while maintaining a consistent internal interface.

The handle is primarily used to:
- Track compiled modules for symbol lookup operations
- Manage resource cleanup when JIT contexts are released  
- Enable lazy compilation where code is only emitted when symbols are first referenced

## Parameters / Member Variables
-  (LLVM > 11): Reference to the LLVM Lazy JIT instance that manages the compiled module
-  (LLVM > 11): Tracks resources associated with the compiled module for proper cleanup
-  (LLVM ≤ 11): Reference to the ORC JIT compilation stack used in older LLVM versions
-  (LLVM ≤ 11): Handle to the specific module within the ORC JIT stack

## Dependencies
- Functions called/Symbols referenced:
  - No direct function calls (struct definition)
- Called from (representative examples):
  - [llvm_release_context](../l/llvm_release_context.md) (src/backend/jit/llvm/llvmjit.c:294)
  - [llvm_get_function](../l/llvm_get_function.md) (src/backend/jit/llvm/llvmjit.c:404, 435)
  - [llvm_compile_module](../l/llvm_compile_module.md) (src/backend/jit/llvm/llvmjit.c:735, 790, 791)

## Notes and Other Information
- The structure uses conditional compilation (#if LLVM_VERSION_MAJOR > 11) to maintain compatibility across different LLVM versions
- Handles are stored in a list within LLVMJitContext for tracking multiple compiled modules
- Memory for handles is allocated in TopMemoryContext to ensure persistence across transaction boundaries
- Resource cleanup varies significantly between LLVM versions, with newer versions requiring explicit resource tracker management
- In LLVM > 11, actual code emission is lazy and occurs during the first symbol lookup, not during module addition to the JIT
- The handle enables symbol address resolution for compiled functions through version-specific lookup mechanisms