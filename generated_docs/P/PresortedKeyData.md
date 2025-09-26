# PresortedKeyData

## Location
[src/include/nodes/execnodes.h:2311-2316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2311-L2316)

## Overview
PresortedKeyData represents information about a sorting key that is already sorted in the input dataset, used as an optimization in multi-key sorting operations to take advantage of existing partial ordering.

## Definition

```c
typedef struct PresortedKeyData
{
	FmgrInfo	flinfo;			/* comparison function info */
	FunctionCallInfo fcinfo;	/* comparison function call info */
	OffsetNumber attno;			/* attribute number in tuple */
} PresortedKeyData;
```
## Detailed Description
PresortedKeyData is used in PostgreSQL's incremental sort optimization to track information about sorting keys that are already sorted in the input data stream. When performing sorting by multiple keys, if the input dataset is already sorted on a prefix of those keys, the system can optimize the sorting process by treating these "presorted keys" specially. This structure stores the necessary function information and attribute details needed to perform comparisons on such presorted keys during incremental sorting operations.

## Parameters / Member Variables
- `flinfo`: Function manager information for the comparison function used to compare values of this key
- `fcinfo`: Function call information structure containing the runtime context for comparison function calls
- `attno`: Offset number identifying which attribute (column) in the tuple this presorted key corresponds to
## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
- Called from (representative examples):
  - [preparePresortedCols](../p/preparePresortedCols.md)
  - [isCurrentGroup](../i/isCurrentGroup.md)
  - [IncrementalSortState](../I/IncrementalSortState.md)

## Notes and Other Information
PresortedKeyData is specifically designed for PostgreSQL's incremental sort node, which optimizes sorting when the input is already partially sorted. The structure enables efficient comparison operations on presorted columns while maintaining the necessary function call context. This optimization is particularly beneficial for queries with ORDER BY clauses where the input data already has some ordering that can be leveraged to reduce sorting work.