# system_nextsampletuple

## Location
[src/backend/access/tablesample/system.c:236-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/tablesample/system.c#L236-L256)

## Overview
This function selects the next sampled tuple in the current block for PostgreSQL's SYSTEM table sampling method, which implements block-level sampling by returning all tuples in each selected block.

## Definition

```c
static OffsetNumber
system_nextsampletuple(SampleScanState *node,
					   BlockNumber blockno,
					   OffsetNumber maxoffset)
```
## Detailed Description
The  function is a core component of PostgreSQL's SYSTEM table sampling method. It implements a simple sequential iteration through all tuples within a selected block. The function advances through each offset number in the block, starting from  and continuing until  is reached.

The function operates under the principle of block sampling, where entire blocks are either fully included or fully excluded from the sample. Once a block is selected, this function ensures all tuples within that block are visited sequentially. The function is designed to be lightweight and doesn't perform visibility checks - those are handled at a higher level by .

When the function reaches the end of a block (exceeds ), it returns  to signal the sampling scan to move to the next block.

## Parameters / Member Variables
- `*node`: Pointer to the  structure containing the sampling state information
- `blockno`: The block number currently being sampled (used for context but not directly used in this function)
- `maxoffset`: The maximum valid offset number in the current block, representing the highest tuple offset that exists
## Dependencies
- Functions called/Symbols referenced:
  -  (structure)
  -  (structure)
  -  (constant)
  -  (constant)
- Called from (representative examples):
  -  (through function pointer assignment)
  - Used indirectly by the table sampling framework

## Notes and Other Information
- This is a static function, meaning it's only accessible within the  file
- The function maintains state through the  field, which tracks the last tuple offset returned
- It's designed to work with PostgreSQL's pluggable table sampling methods (TSM) framework
- The function is stateful - it remembers the last offset returned and advances from there on subsequent calls
- Return value of  serves as a sentinel to indicate block completion
- The function assumes that visibility and tuple existence checks are performed by the caller