# llvm_session_initialize

## Location
[src/backend/jit/llvm/llvmjit.c:864-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L864-L992)

## Overview
Performs one-time per-session initialization of the LLVM JIT compilation environment, setting up target machines, ORC JIT instances, and debug/profiling support.

## Definition

```c
static void
llvm_session_initialize(void)
```
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



## Dependencies
- Functions called/Symbols referenced:
  - [llvm_create_types](llvm_create_types.md) (type system setup)
  - [llvm_set_target](llvm_set_target.md) (target configuration)
  - [llvm_create_jit_instance](llvm_create_jit_instance.md) (JIT instance creation)
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

## Simplified Source

```c
static void
llvm_session_initialize(void)
{
    MemoryContext oldcontext;
    char *cpu, *features;
    LLVMTargetMachineRef opt0_tm, opt3_tm;

    // Skip if already initialized
    if (llvm_session_initialized)
        return;

    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    // Initialize LLVM native target
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
    LLVMInitializeNativeAsmParser();

    // Create LLVM context if needed
    if (llvm_context == NULL) {
        llvm_context = LLVMContextCreate();
        llvm_jit_context_in_use_count = 0;
    }

    // Initialize PostgreSQL types and target info
    llvm_create_types();
    llvm_set_target();

    // Detect host CPU capabilities
    cpu = LLVMGetHostCPUName();
    features = LLVMGetHostCPUFeatures();

    // Create target machines for different optimization levels
    opt0_tm = LLVMCreateTargetMachine(llvm_targetref, llvm_triple, cpu, features,
                                     LLVMCodeGenLevelNone, LLVMRelocDefault,
                                     LLVMCodeModelJITDefault);
    opt3_tm = LLVMCreateTargetMachine(llvm_targetref, llvm_triple, cpu, features,
                                     LLVMCodeGenLevelAggressive, LLVMRelocDefault,
                                     LLVMCodeModelJITDefault);

    // Create ORC JIT instances (version-specific)
    if (LLVM_VERSION_MAJOR > 11) {
        llvm_ts_context = LLVMOrcCreateNewThreadSafeContext();
        llvm_opt0_orc = llvm_create_jit_instance(opt0_tm);
        llvm_opt3_orc = llvm_create_jit_instance(opt3_tm);
    } else {
        llvm_opt0_orc = LLVMOrcCreateInstance(opt0_tm);
        llvm_opt3_orc = LLVMOrcCreateInstance(opt3_tm);
    }

    // Register cleanup callback
    on_proc_exit(llvm_shutdown, 0);
    llvm_session_initialized = true;

    MemoryContextSwitchTo(oldcontext);
}
```