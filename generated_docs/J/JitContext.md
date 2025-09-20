# JitContext

## Location
[src/include/jit/jit.h:57-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/jit.h#L57-L63)

## Overview
JitContext is the main context structure that tracks the state and configuration of JIT compilation operations in PostgreSQL.

## Definition

```c
typedef struct JitContext
{
	/* see PGJIT_* above */
	int			flags;

	JitInstrumentation instr;
} JitContext;
```
## Detailed Description
JitContext serves as the primary coordination structure for PostgreSQL's JIT compilation system. It maintains configuration flags that determine what types of JIT operations should be performed, and includes an embedded JitInstrumentation structure to track performance metrics for the JIT operations. This context is typically associated with query execution state and is used throughout the query lifecycle to guide JIT compilation decisions and collect performance data.

## Parameters / Member Variables
- `flags`: Integer bitfield containing PGJIT_* flags that control JIT compilation behavior (see PGJIT_* constants above in the header file)
- `instr`: Embedded JitInstrumentation structure that tracks JIT compilation performance metrics and statistics

## Dependencies
- Functions called/Symbols referenced:
  - [JitInstrumentation](JitInstrumentation.md) (embedded structure for performance tracking)
- Called from (representative examples):
  - [jit_release_context](../j/jit_release_context.md)
  - [llvm_release_context](../l/llvm_release_context.md)
  - [JitProviderCallbacks](JitProviderCallbacks.md) (as function parameter type)
  - [EState](../E/EState.md) (embedded in execution state)

## Notes and Other Information
- Typically embedded within EState (execution state) to provide JIT context for query execution
- The flags field is a crucial control mechanism that determines the aggressiveness of JIT optimizations
- Used by JIT provider implementations (like LLVM) to maintain provider-specific state
- Provides the interface between PostgreSQL's generic JIT framework and specific JIT implementations
- Central to the cost-based JIT compilation decisions made during query planning and execution