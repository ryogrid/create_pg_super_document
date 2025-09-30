# writetup_datum

## Location
[src/backend/utils/sort/tuplesortvariants.c:1824-1857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1824-L1857)

## Overview
Writes a datum-based SortTuple to a logical tape during external sorting operations, handling both NULL values and different datum storage formats.

## Definition
```c
static void writetup_datum(Tuplesortstate *state, LogicalTape *tape, SortTuple *stup)
```

## Detailed Description
This function serializes a datum-based SortTuple to persistent storage via a LogicalTape during external sorting when memory is insufficient to hold all tuples. It handles three different cases: NULL values (writes no data), pass-by-value datums (writes the Datum directly), and pass-by-reference datums (writes the pointed-to data). The function writes a length prefix before the data and optionally a trailing length suffix for random access support. The serialization format depends on whether the base->tuples field is set, which indicates pass-by-reference storage.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing sort configuration and datum type information
- `tape`: Pointer to the LogicalTape where the tuple data will be written
- `stup`: Pointer to the SortTuple containing the datum to be written

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [datumGetSize](../d/datumGetSize.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [LogicalTapeWrite](../L/LogicalTapeWrite.md)
- Called from (representative examples):
  - [tuplesort_begin_datum](../t/tuplesort_begin_datum.md)
  - CLUSTER_SORT

## Notes and Other Information
The function implements a space-efficient serialization format by only writing the actual data size rather than a fixed-size buffer. For pass-by-reference datums, it uses datumGetSize() to determine the correct number of bytes to write. The trailing length word is only written when TUPLESORT_RANDOMACCESS option is enabled, allowing backward traversal of the tape. This function is part of the external sorting mechanism that allows PostgreSQL to sort datasets larger than available memory.

## Simplified Source

```c
static void writetup_datum(Tuplesortstate *state, LogicalTape *tape, SortTuple *stup) {
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    TuplesortDatumArg *arg = (TuplesortDatumArg *) base->arg;
    void *write_data;
    unsigned int data_length;
    unsigned int total_length;

    // Determine what data to write based on tuple type
    if (stup->isnull1) {
        // NULL value - write nothing
        write_data = NULL;
        data_length = 0;
    } else if (!base->tuples) {
        // Pass-by-value datum - write the datum directly
        write_data = &stup->datum1;
        data_length = sizeof(Datum);
    } else {
        // Pass-by-reference datum - write the pointed-to data
        write_data = stup->tuple;
        data_length = datumGetSize(PointerGetDatum(stup->tuple), false, arg->datumTypeLen);
    }

    // Calculate total length including length prefix
    total_length = data_length + sizeof(unsigned int);

    // Write length prefix, then data
    LogicalTapeWrite(tape, &total_length, sizeof(total_length));
    LogicalTapeWrite(tape, write_data, data_length);

    // Write trailing length for random access support
    if (base->sortopt & TUPLESORT_RANDOMACCESS)
        LogicalTapeWrite(tape, &total_length, sizeof(total_length));
}
```