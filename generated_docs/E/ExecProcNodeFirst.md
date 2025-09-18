# ExecProcNodeFirst

## Location
[src/backend/executor/execProcnode.c:443-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execProcnode.c#L443-L473)

## Overview
ExecProcNodeFirst is a one-time wrapper function that performs initial setup checks before delegating to the actual execution function, optimizing subsequent calls by removing unnecessary overhead.

## Definition


## Detailed Description
ExecProcNodeFirst serves as an intelligent bootstrap wrapper for plan node execution. It is designed to perform expensive one-time setup operations during the first execution of a plan node, then optimize subsequent executions by removing itself from the call chain.

The function performs two critical first-time operations:
1. Stack depth checking to prevent stack overflow - this check is expensive on some architectures (like x86), so it's only done once under the assumption that all ExecProcNode calls for a given node will occur at roughly the same stack depth
2. Instrumentation setup - if performance instrumentation is enabled, it installs ExecProcNodeInstr as the ongoing wrapper; otherwise, it sets up direct calls to the real execution function

After performing these checks, the function updates the node's ExecProcNode pointer to bypass itself on future calls, creating an efficient execution path. This self-removing wrapper pattern ensures that the overhead of setup checks doesn't impact the performance of long-running queries.

## Parameters / Member Variables
- : The PlanState node being executed for the first time

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow prevention)
  - [ExecProcNodeInstr](ExecProcNodeInstr.md) (instrumentation wrapper function)
  - ExecProcNode (execution procedure field)
- Called from (representative examples):
  - [ExecSetExecProcNode](ExecSetExecProcNode.md) (sets this as initial wrapper)

## Notes and Other Information
- This is a static function, only visible within execProcnode.c
- Returns a TupleTableSlot pointer like all ExecProcNode functions
- The function modifies the node's ExecProcNode pointer to optimize future calls
- Stack depth checking is done only once due to performance considerations on certain architectures
- After the first call, the function is bypassed, making subsequent executions more efficient
- The optimization assumes consistent stack depth across executions of the same node