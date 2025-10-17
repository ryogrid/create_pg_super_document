# text_to_array

## Location
[src/backend/utils/adt/varlena.c:4514-4539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4514-L4539)

## Overview
Parses input string and returns a text array of elements based on a provided field separator.

## Definition

```c
struct_empty_array(TEXTOID));
```
## Detailed Description
The text_to_array function is a PostgreSQL built-in function that splits a text string into an array of text elements using a specified delimiter. It initializes a SplitTextOutputData structure and delegates the actual splitting logic to the split_text function. If the splitting operation fails or produces no elements, it handles these cases appropriately by returning NULL or an empty array respectively.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SplitTextOutputData (structure for output state)
  - [split_text](../s/split_text.md) (core text splitting logic)
  - [construct_empty_array](../c/construct_empty_array.md) (creates empty array when no elements)
  - PG_RETURN_ARRAYTYPE_P (macro for returning array type)
  - [makeArrayResult](../m/makeArrayResult.md) (converts array state to result)
  - PG_RETURN_DATUM (macro for returning datum)
- Called from (representative examples):
  - [text_to_array_null](text_to_array_null.md)

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:4514-4539
- Uses memset to initialize the output state structure to all zeroes
- Handles edge cases by returning NULL for failed splits and empty arrays for no elements
- Part of PostgreSQL's variable-length data type utilities

## Simplified Source

```c
Datum text_to_array(PG_FUNCTION_ARGS) {
    // Initialize output state for array construction
    SplitTextOutputData tstate;
    memset(&tstate, 0, sizeof(tstate));

    // Delegate to split_text for the actual parsing work
    if (!split_text(fcinfo, &tstate))
        PG_RETURN_NULL();

    // Handle empty result case
    if (tstate.astate == NULL)
        PG_RETURN_ARRAYTYPE_P(construct_empty_array(TEXTOID));

    // Convert accumulated array state to final result
    PG_RETURN_DATUM(makeArrayResult(tstate.astate, CurrentMemoryContext));
}
```