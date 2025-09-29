# ExecShutdownHash

## Location
[src/backend/executor/nodeHash.c:2811-2825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2811-L2825)

## Overview
ExecShutdownHash collects and saves hash table instrumentation statistics before the node shuts down, ensuring data is preserved in both parallel and non-parallel execution contexts.

## Definition
```c
void ExecShutdownHash(HashState *node)
```

## Detailed Description
This function is responsible for collecting EXPLAIN statistics from hash table operations just before the hash node shuts down. It serves a critical role in preserving instrumentation data that would otherwise be lost when the execution context is destroyed.

The function handles two scenarios:
1. **Non-parallel execution**: If no shared memory instrumentation area exists (hinstrument is NULL), it allocates local storage for the statistics
2. **Parallel execution**: If a shared memory area was established by ExecHashInitializeWorker, it uses that existing space

The timing of this function is crucial in parallel queries - it must run during shutdown rather than during ExecEndHash() because ExecEndHash() executes after the process has detached from the Dynamic Shared Memory (DSM) segment, making the shared instrumentation data inaccessible.

After ensuring storage is available, the function calls ExecHashAccumInstrumentation to collect the final statistics from the current hash table.

## Parameters / Member Variables
- `node`: HashState pointer containing the hash node state, instrumentation settings, and hash table reference

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_object
  - [ExecHashAccumInstrumentation](ExecHashAccumInstrumentation.md)
- Types used:
  - [HashState](../H/HashState.md)
  - [HashInstrumentation](../H/HashInstrumentation.md)
- Called from (representative examples):
  - [ExecShutdownNode_walker](ExecShutdownNode_walker.md)

## Notes and Other Information
- The function only allocates local storage if instrumentation is enabled (node->ps.instrument is not NULL) and no instrumentation area exists yet
- In parallel execution, the hinstrument pointer should already be set by ExecHashInitializeWorker to point to shared memory
- The final statistics collection only occurs if both hinstrument and hashtable are valid
- This function is part of PostgreSQL's execution node shutdown sequence and is essential for accurate EXPLAIN ANALYZE output
- The use of palloc0_object ensures the allocated HashInstrumentation structure is zero-initialized
- The timing difference between ExecShutdownHash() and ExecEndHash() is critical for parallel query instrumentation data collection

## Simplified Source

```c
void ExecShutdownHash(HashState *node) {
    // Ensure instrumentation storage exists if EXPLAIN is enabled
    if (node->ps.instrument && !node->hinstrument) {
        // Allocate local storage for instrumentation data
        node->hinstrument = palloc0_object(HashInstrumentation);
    }

    // Collect final hash table statistics
    if (node->hinstrument && node->hashtable) {
        ExecHashAccumInstrumentation(node->hinstrument, node->hashtable);
    }
}
```