# system_initsamplescan

## Location
src/backend/access/tablesample/system.c: 130 - 138

## Overview
Initializes the sampling state structure for the SYSTEM table sampling method during executor setup, allocating memory for the SystemSamplerData structure.

## Definition
```c
static void system_initsamplescan(SampleScanState *node, int eflags)
```

## Detailed Description
This function is called during the initialization phase of query execution to set up the necessary data structures for SYSTEM table sampling. It allocates and zero-initializes a SystemSamplerData structure that will hold the sampling state throughout the scan operation. The SystemSamplerData structure maintains important sampling parameters such as the hash cutoff value, random seed, current block position, and last tuple offset. This initialization ensures that the sampling algorithm has a clean state to work with when the actual scanning begins.

## Parameters / Member Variables
- `node`: SampleScanState structure representing the sample scan node in the execution tree
- `eflags`: Executor flags indicating special execution requirements (currently unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - SystemSamplerData (structure type for sampling state)
- Called from (representative examples):
  - [tsm_system_handler](../t/tsm_system_handler.md) (as function pointer in TsmRoutine)
  - PostgreSQL executor during sample scan initialization

## Notes and Other Information
- The function uses palloc0 to ensure the SystemSamplerData structure is initialized with zeros
- This is a minimal initialization function that only allocates memory; actual parameter setup occurs in system_beginsamplescan
- The allocated SystemSamplerData structure contains fields for cutoff value, random seed, next block number, and last tuple offset
- Memory allocated here will be automatically freed when the query execution context is destroyed
- The eflags parameter is provided for potential future use but is currently ignored