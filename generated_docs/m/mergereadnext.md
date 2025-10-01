# mergereadnext

## Location
[src/backend/utils/sort/tuplesort.c:2320-2338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2320-L2338)

## Overview
Reads the next tuple from a specified merge input tape during tuple sorting merge operations, returning false when end of file is reached.

## Definition
```c
static bool mergereadnext(Tuplesortstate *state, LogicalTape *srcTape, SortTuple *stup)
```

## Detailed Description
The mergereadnext function is a core component of the merge phase in PostgreSQL's tuple sorting algorithm. It reads the next tuple from a logical tape by first obtaining the tuple length using getlen() and then reading the actual tuple data using the READTUP macro. The function serves as an abstraction layer for tape reading operations during merge passes, handling the low-level details of tuple extraction from tape storage. If no more tuples are available (tuplen == 0), it signals EOF by returning false.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure containing sorting context and configuration
- `srcTape`: Pointer to the LogicalTape from which to read the next tuple
- `stup`: Pointer to SortTuple structure where the read tuple data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](../L/LogicalTape.md) (tape abstraction structure)
  - [Tuplesortstate](../T/Tuplesortstate.md) (sorting state structure)
  - SortTuple (tuple container structure)
  - [getlen](../g/getlen.md) (reads tuple length from tape)
  - READTUP (macro for reading tuple data)
- Called from (representative examples):
  - [tuplesort_gettuple_common](../t/tuplesort_gettuple_common.md)
  - [mergeonerun](mergeonerun.md)
  - [beginmerge](../b/beginmerge.md)

## Notes and Other Information
- Returns true if a tuple was successfully read, false on EOF
- This is a static function internal to tuplesort.c
- Uses getlen with true parameter to handle EOF detection
- The READTUP macro handles the actual tuple data reading based on tuple type
- Critical for maintaining proper merge heap state during multi-way merge operations

## Simplified Source

```c
static bool
mergereadnext(Tuplesortstate *state, LogicalTape *srcTape, SortTuple *stup)
{
    unsigned int tuplen;

    // Read tuple length from tape
    if ((tuplen = getlen(srcTape, true)) == 0)
        return false;  // EOF reached

    // Read the actual tuple data
    READTUP(state, stup, srcTape, tuplen);

    return true;  // Successfully read tuple
}
```