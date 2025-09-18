# hashagg_batch_new

## Location
src/backend/executor/nodeAgg.c: 2991 - 3009

## Overview
Constructs and initializes a new HashAggBatch structure that represents one iteration of hash aggregation processing to be performed.

## Definition
```c
static HashAggBatch *hashagg_batch_new(LogicalTape *input_tape, int setno, int64 input_tuples, double input_card, int used_bits)
```

## Detailed Description
This function creates and initializes a HashAggBatch structure, which encapsulates all the information needed to process one batch of spilled hash aggregation data. The batch represents a unit of work that will be processed later when reading spilled partitions back from disk. Each batch corresponds to a specific grouping set and contains metadata about the input data characteristics.

The HashAggBatch structure serves as a work item in the hash aggregation's multi-pass processing strategy, where spilled data is processed in batches to manage memory usage effectively. The batch includes statistical information that helps with memory planning and hash table sizing for the subsequent processing phase.

## Parameters / Member Variables
- `input_tape`: LogicalTape containing the spilled tuples for this batch
- `setno`: Grouping set number that this batch belongs to
- `input_tuples`: Estimated number of tuples in this batch
- `input_card`: Estimated cardinality (number of distinct groups) in this batch  
- `used_bits`: Number of hash bits already consumed in previous partitioning levels

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [HashAggBatch](../H/HashAggBatch.md) (structure type)
- Called from (representative examples):
  - [hashagg_spill_finish](hashagg_spill_finish.md)

## Notes and Other Information
- The function performs a simple allocation and initialization of the HashAggBatch structure
- All input parameters are directly copied into the corresponding batch fields
- The batch structure is allocated using `palloc0` which zero-initializes all fields
- This is a lightweight constructor function that prepares work items for later batch processing
- The `used_bits` parameter is important for recursive partitioning when a batch itself needs to be spilled again