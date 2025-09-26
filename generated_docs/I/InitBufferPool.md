# InitBufferPool

## Location
src/backend/storage/buffer/buf_init.c: 68 - 159

## Overview
InitBufferPool initializes the shared buffer pool during PostgreSQL's shared-memory initialization phase, setting up buffer descriptors, buffer blocks, condition variables, and checkpoint-related data structures.

## Definition
void InitBufferPool(void)

## Detailed Description
InitBufferPool is a critical initialization function that sets up PostgreSQL's shared buffer pool infrastructure during system startup. The function performs several key tasks:

1. **Buffer Descriptors Allocation**: Creates an array of BufferDescPadded structures, aligned to cacheline boundaries for optimal performance. Each descriptor contains metadata about a buffer including its tag, state, and synchronization information.

2. **Buffer Blocks Allocation**: Allocates the actual buffer memory blocks aligned to IO page size boundaries (PG_IO_ALIGN_SIZE). This alignment is crucial for efficient disk I/O operations.

3. **Condition Variables Setup**: Initializes condition variables for buffer I/O synchronization, allowing processes to efficiently wait for buffer operations to complete.

4. **Checkpoint Infrastructure**: Sets up the CkptBufferIds array used during checkpointing to sort buffer IDs that need to be written to disk.

5. **Buffer Initialization**: If this is a fresh initialization (not EXEC_BACKEND case), the function initializes each buffer descriptor with default values, links them into a free list, and sets up their locks and condition variables.

6. **Strategy and Writeback Initialization**: Calls StrategyInitialize to set up buffer replacement strategy and initializes the backend writeback context for file flushing.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitStruct (shared memory allocation)
  - TYPEALIGN (memory alignment)
  - GetBufferDescriptor (buffer access)
  - ClearBufferTag (buffer tag initialization)
  - pg_atomic_init_u32 (atomic variable initialization)
  - LWLockInitialize (lock initialization)
  - BufferDescriptorGetContentLock (lock access)
  - ConditionVariableInit (condition variable setup)
  - BufferDescriptorGetIOCV (condition variable access)
  - StrategyInitialize (buffer strategy setup)
  - WritebackContextInit (writeback initialization)
- Called from (representative examples):
  - CreateOrAttachShmemStructs (main initialization path)

## Notes and Other Information
- This function is called exactly once during PostgreSQL startup, either in the postmaster process or in a standalone backend
- The function handles both fresh initialization and reattachment to existing shared memory (EXEC_BACKEND case)
- Buffer descriptors are aligned to cacheline boundaries to minimize false sharing between CPU cores
- Buffer blocks are aligned to I/O boundaries for optimal disk performance
- The free list linking (freeNext) is later managed by freelist.c
- Memory allocation failures during initialization are fatal to the system
- The checkpoint buffer IDs array is pre-allocated to avoid memory allocation during checkpoints when the system is under stress