# split_text_accum_result

## Location
[src/backend/utils/adt/varlena.c:4727-4765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4727-L4765)

## Overview
Helper function for text splitting operations that adds individual text items to a result set (either tuple store or array accumulator), handling null value detection based on a null string pattern.

## Definition

```c
static void
split_text_accum_result(SplitTextOutputData *tstate,
						text *field_value,
						text *null_string,
						Oid collation)
```
## Detailed Description
This internal function is responsible for accumulating text fields during string splitting operations. It serves two main purposes: checking if the input field matches a specified null string pattern and adding the field to the appropriate result container. The function supports two output modes - storing results in a tuple store for table output or in an array accumulator for array output. When a field value matches the null_string parameter (using collation-aware comparison), it treats the field as NULL rather than storing the actual text value.

## Parameters / Member Variables
- `*tstate`: Pointer to SplitTextOutputData structure containing output state information including tuple store or array accumulator
- `*field_value`: The text field to be added to the result set
- `*null_string`: Optional text pattern that, when matched, indicates the field should be treated as NULL
- `collation`: Object ID specifying the collation rules for text comparison operations
## Dependencies
- Functions called/Symbols referenced:
  - [text_isequal](../t/text_isequal.md) (for comparing field_value with null_string)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (for adding values to tuple store)
  - [accumArrayResult](../a/accumArrayResult.md) (for adding values to array accumulator)
  - [PointerGetDatum](../P/PointerGetDatum.md) (for converting text pointer to Datum)
  - CurrentMemoryContext (for memory management)
- Called from:
  - [split_text](split_text.md) (multiple times during text splitting operations)

## Notes and Other Information
This function is part of PostgreSQL's string manipulation infrastructure, specifically used by the split_text function family. The dual-mode operation (tuple store vs array accumulator) allows the same splitting logic to support both table-valued functions and array-returning functions. The null string comparison uses proper collation rules to ensure consistent behavior across different locale settings. The function is marked as static, indicating it's an internal implementation detail not exposed to external callers.

## Simplified Source

```c
static void split_text_accum_result(SplitTextOutputData *tstate,
                                   text *field_value,
                                   text *null_string,
                                   Oid collation) {
    // Check if this field should be treated as NULL
    bool is_null = false;
    if (null_string && text_isequal(field_value, null_string, collation))
        is_null = true;

    if (tstate->tupstore) {
        // Table output mode - store in tuple store
        Datum values[1];
        bool nulls[1];

        values[0] = PointerGetDatum(field_value);
        nulls[0] = is_null;

        tuplestore_putvalues(tstate->tupstore, tstate->tupdesc, values, nulls);
    } else {
        // Array output mode - accumulate in array state
        tstate->astate = accumArrayResult(tstate->astate,
                                         PointerGetDatum(field_value),
                                         is_null,
                                         TEXTOID,
                                         CurrentMemoryContext);
    }
}
```