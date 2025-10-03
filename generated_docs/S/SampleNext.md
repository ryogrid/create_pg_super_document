# SampleNext

## Location
[src/backend/executor/nodeSamplescan.c:42-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSamplescan.c#L42-L59)

## Overview
SampleNext is an internal helper function that retrieves the next tuple from a table sampling scan operation, handling initialization on the first call and delegating to the sampling method's specific tuple retrieval logic.

## Definition

```c
static TupleTableSlot *
SampleNext(SampleScanState *node)
```
## Detailed Description
SampleNext serves as the core workhorse function for sample scanning operations in PostgreSQL's executor. It implements a lazy initialization pattern where the sampling operation is initialized only on the first call within a scan. Once initialized, it delegates to the specific table sampling method (via tablesample_getnext) to retrieve the next tuple that satisfies the sampling criteria. The function returns a TupleTableSlot containing the sampled tuple, or NULL when no more tuples are available from the sample.

## Parameters / Member Variables
- : A pointer to the SampleScanState structure that maintains the state of the sample scan operation, including the sampling method, parameters, and current scan position.

## Dependencies
- Functions called/Symbols referenced:
  - [tablesample_init](../t/tablesample_init.md)
  - [tablesample_getnext](../t/tablesample_getnext.md)
  - [SampleScanState](SampleScanState.md)
- Called from (representative examples):
  - [ExecSampleScan](../E/ExecSampleScan.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the nodeSamplescan.c file
- Uses lazy initialization pattern - the sampling operation is only initialized on the first call via the  flag in the SampleScanState
- Acts as an abstraction layer between the executor's sample scan node and the specific table sampling method implementation
- The function's simplicity allows different sampling methods to plug into the same interface

## Simplified Source

```c
static TupleTableSlot *
SampleNext(SampleScanState *node)
{
    // Initialize sampling on first call
    if (!node->begun)
        tablesample_init(node);

    // Get next tuple from sampling method
    return tablesample_getnext(node);
}
```