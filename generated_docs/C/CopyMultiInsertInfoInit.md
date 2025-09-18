# CopyMultiInsertInfoInit

## Location
[src/backend/commands/copyfrom.c:258-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L258-L282)

## Overview
CopyMultiInsertInfoInit initializes a pre-allocated CopyMultiInsertInfo structure with the necessary state and parameters for multi-insert operations, optionally setting up a buffer for non-partitioned tables.

## Definition


## Detailed Description
This function initializes the CopyMultiInsertInfo structure that coordinates multi-insert operations during COPY FROM. The initialization process includes:

1. **Buffer management initialization**: Sets up empty lists and counters for tracking buffered tuples and bytes
2. **State preservation**: Stores references to critical execution state objects (CopyFromState, EState)
3. **Command tracking**: Records the command ID and table insert options for the operation
4. **Conditional buffer setup**: For non-partitioned tables, immediately creates a CopyMultiInsertBuffer

The function implements an important optimization strategy: regular tables get their buffers set up immediately, while partitioned tables have their buffers created lazily (only when needed). This is because partitioned tables may route tuples to different partitions, so buffers are created on-demand for each partition that actually receives data.

The bufferedTuples and bufferedBytes counters start at zero and will track the total number of tuples and bytes across all buffers managed by this CopyMultiInsertInfo structure.

## Parameters / Member Variables
- : Pointer to the CopyMultiInsertInfo structure to be initialized
- : Pointer to ResultRelInfo representing the target relation
- : Pointer to CopyFromState containing the current state of the COPY operation
- : Pointer to EState providing execution context and environment
- : CommandId identifying the current command for visibility and locking purposes
- : Integer containing table insert options that control insertion behavior

## Dependencies
- Functions called/Symbols referenced:
  - [CopyMultiInsertInfo](CopyMultiInsertInfo.md) (struct type)
  - CopyFromState (struct type)
  - CommandId (type alias)
  - NIL (empty list constant)
  - [CopyMultiInsertInfoSetupBuffer](CopyMultiInsertInfoSetupBuffer.md) (buffer setup function)
  - RELKIND_PARTITIONED_TABLE (constant for partitioned table identification)
- Called from (representative examples):
  - [CopyFrom](CopyFrom.md) (main COPY FROM function at src/backend/commands/copyfrom.c:916)

## Notes and Other Information
- This is a static function accessible only within copyfrom.c
- The function implements lazy buffer creation for partitioned tables as an optimization
- Buffer setup is conditional based on relation kind - partitioned tables defer buffer creation until needed
- All numeric counters (bufferedTuples, bufferedBytes) start at zero
- The multiInsertBuffers list starts as NIL and will be populated as buffers are created
- [Command](Command.md) ID tracking is essential for proper transaction visibility and concurrency control
- The ti_options parameter allows fine-grained control over insertion behavior and optimizations