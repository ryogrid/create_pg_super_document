# llvm_session_initialize

## Location
src/backend/jit/llvm/llvmjit.c: 864 - 992

## Overview
Performs one-time per-session initialization of the LLVM JIT compilation environment, setting up target machines, ORC JIT instances, and debug/profiling support.

## Definition


## Detailed Description
This function initializes the LLVM JIT compilation infrastructure for the current PostgreSQL session. It is designed to be called only once per session and includes:

1. **Native Target Initialization**: Initializes LLVM's native target, ASM printer, and parser
2. **Context Management**: Creates or reuses the global LLVM context with proper opaque pointer settings based on LLVM version
3. **Type Synchronization**: Creates PostgreSQL-specific LLVM types and infers the target triple
4. **Target Machine Creation**: Creates two target machines with different optimization levels:
   - : No optimization (LLVMCodeGenLevelNone)
   - : Aggressive optimization (LLVMCodeGenLevelAggressive)
5. **Host CPU Detection**: Automatically detects the host CPU and its features for optimal code generation
6. **ORC JIT Setup**: Creates ORC JIT instances for both optimization levels with version-specific handling:
   - **LLVM > 11**: Uses ThreadSafeContext and new ORC API
   - **LLVM ≤ 11**: Uses legacy ORC API with optional GDB and Perf event listeners
7. **Cleanup Registration**: Registers llvm_shutdown for process exit cleanup

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [llvm_create_types](llvm_create_types.md) (type system setup)
  - [llvm_set_target](llvm_set_target.md) (target configuration)
  - llvm_create_jit_instance (JIT instance creation)
  - [llvm_shutdown](llvm_shutdown.md) (cleanup function)
  - [on_proc_exit](../o/on_proc_exit.md) (process exit handler registration)
- Called from (representative examples):
  - [llvm_create_context](llvm_create_context.md)

## Notes and Other Information
- The function uses a guard variable  to prevent multiple initializations
- Memory allocation is performed in TopMemoryContext to persist across PostgreSQL memory context switches  
- For LLVM 15, opaque pointers are explicitly disabled for compatibility
- Host CPU and feature detection ensures generated code uses all available CPU capabilities
- Debug and profiling support is conditionally enabled based on compile-time feature detection
- The function handles significant version differences between LLVM > 11 and ≤ 11
- Library symbol loading is forced with  to ensure all main binary symbols are available
- Proper resource cleanup is ensured by disposing of temporary CPU and feature strings
- Debug logging (DEBUG2 level) provides information about detected CPU and features