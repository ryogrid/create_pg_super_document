# heap_form_minimal_tuple

## Location
[src/backend/access/common/heaptuple.c:1452-1522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1452-L1522)

## Overview
Constructs a MinimalTuple from arrays of values and null indicators, creating a compact tuple representation without a HeapTupleData header or system columns.

## Definition
```c
MinimalTuple heap_form_minimal_tuple(TupleDesc tupleDescriptor,
                                    const Datum *values,
                                    const bool *isnull)
```

## Detailed Description
The `heap_form_minimal_tuple` function creates a MinimalTuple structure from provided values and null indicators. This is similar to `heap_form_tuple()` but produces a "minimal" tuple that lacks the HeapTupleData header and space for system columns, making it more memory-efficient for temporary storage and tuple manipulation operations.

The function performs several key operations:
1. Validates that the number of attributes doesn't exceed PostgreSQL's limits
2. Checks for null values in the input arrays
3. Calculates the total space needed including header, null bitmap (if needed), and data
4. Allocates and initializes the MinimalTuple structure
5. Uses `heap_fill_tuple()` to populate the data portion

The result is allocated in the current memory context and must be freed by the caller when no longer needed.

## Parameters / Member Variables
- `tupleDescriptor`: TupleDesc describing the structure and types of the tuple attributes
- `values`: Array of Datum values for each attribute (length must match tupleDescriptor->natts)
- `isnull`: Array of boolean flags indicating which values are null (length must match tupleDescriptor->natts)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_compute_data_size](heap_compute_data_size.md)
  - [heap_fill_tuple](heap_fill_tuple.md)
  - [palloc0](../p/palloc0.md)
  - ereport
  - HeapTupleHeaderSetNatts
  - BITMAPLEN
  - MAXALIGN
- Called from (representative examples):
  - [tts_virtual_copy_minimal_tuple](../t/tts_virtual_copy_minimal_tuple.md)
  - [tts_minimal_materialize](../t/tts_minimal_materialize.md)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- More memory-efficient than regular HeapTuples due to lack of HeapTupleData header
- Commonly used in executor tuple table slots and temporary tuple storage
- The function enforces PostgreSQL's MaxTupleAttributeNumber limit for tuple width
- Includes null bitmap only when necessary (when at least one attribute is null)
- The resulting MinimalTuple must be freed using appropriate memory management functions
- Used primarily in query execution and tuple manipulation contexts where space efficiency is important

## Simplified Source

```c
MinimalTuple
heap_form_minimal_tuple(TupleDesc tupleDescriptor,
                        const Datum *values,
                        const bool *isnull)
{
    MinimalTuple tuple;
    Size len, data_len;
    int hoff;
    bool hasnull = false;
    int numberOfAttributes = tupleDescriptor->natts;
    int i;

    // Check attribute count limit
    if (numberOfAttributes > MaxTupleAttributeNumber)
        ereport(ERROR, "number of columns (%d) exceeds limit (%d)");

    // Check if any attributes are null
    for (i = 0; i < numberOfAttributes; i++) {
        if (isnull[i]) {
            hasnull = true;
            break;
        }
    }

    // Calculate space needed
    len = SizeofMinimalTupleHeader;

    // Add space for null bitmap if needed
    if (hasnull)
        len += BITMAPLEN(numberOfAttributes);

    // Align header and calculate data size
    hoff = len = MAXALIGN(len);
    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);
    len += data_len;

    // Allocate and initialize minimal tuple
    tuple = (MinimalTuple) palloc0(len);

    // Set tuple header fields
    tuple->t_len = len;
    HeapTupleHeaderSetNatts(tuple, numberOfAttributes);
    tuple->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;

    // Fill in the tuple data
    heap_fill_tuple(tupleDescriptor, values, isnull,
                    (char *) tuple + hoff, data_len,
                    &tuple->t_infomask,
                    (hasnull ? tuple->t_bits : NULL));

    return tuple;
}
```