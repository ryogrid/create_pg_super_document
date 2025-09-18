# ExecEndSort

## Location
[src/backend/executor/nodeSort.c:301-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L301-L328)

## Overview
Cleans up and releases resources used by a Sort plan node, including tuplesort state and child plan nodes.

## Definition


## Detailed Description
ExecEndSort is the cleanup function for Sort plan nodes in PostgreSQL's executor. It performs orderly shutdown of the sort node by:

1. **Tuplesort Resource Cleanup**: Releases all resources held by the tuplesort module, including temporary files, memory buffers, and any intermediate sort state. This is accomplished by calling  if a tuplesort state exists.

2. **Child Node Cleanup**: Recursively shuts down the outer child plan node to ensure proper cleanup of the entire execution subtree.

3. **State Reset**: Sets the tuplesortstate pointer to NULL to prevent any accidental access to freed resources.

This function is critical for preventing resource leaks, especially when dealing with large sorts that may have created temporary files on disk or consumed significant amounts of memory.

## Parameters / Member Variables
- : The SortState structure containing the sort execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - : Release tuplesort resources and cleanup temporary files/memory
  - : Recursively cleanup the outer child plan node
  - : Access the outer plan state for cleanup
- Called from (representative examples):
  - : During executor shutdown phase

## Notes and Other Information
- Function includes debugging output via SO1_printf for development builds
- Essential for proper resource management in queries involving large datasets or temporary file usage
- Must be called even if the sort operation was never completed (e.g., due to early termination)
- Part of the standard executor lifecycle: ExecInitSort → ExecSort → ExecEndSort