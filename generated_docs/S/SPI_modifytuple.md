# SPI_modifytuple

## Location
[src/backend/executor/spi.c:1106-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1106-L1174)

## Overview
Creates a modified copy of a tuple by replacing specified attribute values, used for updating tuples in stored procedures and trigger functions.

## Definition

```c
HeapTuple
SPI_modifytuple(Relation rel, HeapTuple tuple, int natts, int *attnum,
				Datum *Values, const char *Nulls)
```
## Detailed Description
SPI_modifytuple creates a new HeapTuple that is a copy of the input tuple with specified attributes modified to new values. This function is commonly used in trigger functions and stored procedures where you need to modify some fields of a tuple while preserving others. The function decomposes the original tuple, replaces the specified attribute values, and then reconstructs a new tuple with the modified data.

The function preserves important tuple identification information (t_ctid, t_self, and t_tableOid) from the original tuple to the modified tuple. It performs comprehensive validation of input parameters and attribute numbers to ensure data integrity.

## Parameters / Member Variables
- `rel`: The relation (table) that defines the tuple structure and attribute information
- `tuple`: The original HeapTuple to be modified
- `natts`: The number of attributes to be modified
- `*attnum`: Array of 1-based attribute numbers indicating which attributes to modify
- `*Values`: Array of new Datum values to replace the existing attribute values
- `*Nulls`: Optional array of null indicators ('n' means null, anything else means not null)
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ttdummy](../t/ttdummy.md) (trigger function in regression tests)
  - Various custom trigger functions
  - Stored procedures that need to modify tuple data

## Notes and Other Information
- Sets SPI_result to SPI_ERROR_ARGUMENT if any required parameter is NULL or natts < 0
- Sets SPI_result to SPI_ERROR_UNCONNECTED if no SPI connection is active
- Sets SPI_result to SPI_ERROR_NOATTRIBUTE if any attribute number is invalid (≤ 0 or > number of attributes)
- Attribute numbers are 1-based, not 0-based
- The Nulls parameter can be NULL if no attributes should be set to null
- Returns NULL on any error condition
- The returned tuple is allocated in the upper executor's memory context
- Commonly used in BEFORE UPDATE triggers to modify the NEW tuple

## Simplified Source

```c
HeapTuple SPI_modifytuple(Relation rel, HeapTuple tuple, int natts, int *attnum,
                         Datum *Values, const char *Nulls) {
    // Validate input parameters
    if (rel == NULL || tuple == NULL || natts < 0 || attnum == NULL || Values == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    // Check SPI connection
    if (_SPI_current == NULL) {
        SPI_result = SPI_ERROR_UNCONNECTED;
        return NULL;
    }

    // Switch to saved memory context
    MemoryContext old_context = MemoryContextSwitchTo(_SPI_current->savedcxt);
    SPI_result = 0;

    // Allocate arrays for all attribute values and nulls
    int num_attrs = rel->rd_att->natts;
    Datum *values = palloc(num_attrs * sizeof(Datum));
    bool *nulls = palloc(num_attrs * sizeof(bool));

    // Extract existing values from tuple
    heap_deform_tuple(tuple, rel->rd_att, values, nulls);

    // Replace specified attributes
    HeapTuple modified_tuple = NULL;
    for (int i = 0; i < natts; i++) {
        if (attnum[i] <= 0 || attnum[i] > num_attrs) {
            SPI_result = SPI_ERROR_NOATTRIBUTE;
            break;
        }
        values[attnum[i] - 1] = Values[i];
        nulls[attnum[i] - 1] = (Nulls && Nulls[i] == 'n');
    }

    // Create modified tuple if no errors occurred
    if (SPI_result == 0) {
        modified_tuple = heap_form_tuple(rel->rd_att, values, nulls);

        // Copy identification info from original tuple
        modified_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
        modified_tuple->t_self = tuple->t_self;
        modified_tuple->t_tableOid = tuple->t_tableOid;
    }

    // Clean up and restore context
    pfree(values);
    pfree(nulls);
    MemoryContextSwitchTo(old_context);

    return modified_tuple;
}
```