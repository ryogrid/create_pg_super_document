# tidin

## Location
[src/backend/utils/adt/tid.c:52-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L52-L118)

## Overview
The `tidin` function is the input conversion function for PostgreSQL's TID (tuple identifier) data type, converting a string representation of a TID into its internal ItemPointer format.

## Definition
```c
Datum tidin(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tidin` function parses a string representation of a TID in the format "(block,offset)" and converts it to an internal ItemPointer structure. The function performs extensive validation to ensure the input string follows the correct syntax and that both block and offset numbers are within valid ranges. It uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS) and supports error context for better error reporting.

The function expects input in the format "(block_number,offset_number)" where:
- block_number is a valid BlockNumber (typically 32-bit unsigned integer)
- offset_number is a valid OffsetNumber (16-bit unsigned integer, max USHRT_MAX)
- The parentheses and comma are required delimiters

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_CSTRING(0)`: String representation of TID to parse
- `escontext`: Error context from function call info for enhanced error reporting
- Internal variables:
  - `coord[NTIDARGS]`: Array to store pointers to coordinate substrings (block and offset)
  - `blockNumber`: Parsed block number as BlockNumber type
  - `offsetNumber`: Parsed offset number as OffsetNumber type
  - `result`: Allocated ItemPointer to return

## Dependencies
- Functions called/Symbols referenced:
  - `strtoul`: Standard library function for string to unsigned long conversion
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [ItemPointerSet](../I/ItemPointerSet.md): Sets block and offset in ItemPointer structure
  - `PG_RETURN_ITEMPOINTER`: PostgreSQL macro to return ItemPointer datum
  - `ereturn`: Error return function with context support
- Constants used:
  - `NTIDARGS` (value: 2): Number of expected TID arguments
  - `LDELIM` (value: '('): Left delimiter character
  - `RDELIM` (value: ')'): Right delimiter character  
  - `DELIM` (value: ','): Coordinate delimiter character
- Called from (representative examples):
  - PostgreSQL type system when converting string literals to TID values
  - SQL parsing and execution when TID constants are encountered

## Notes and Other Information
- The function includes special handling for platforms where `unsigned long` is wider than `BlockNumber` to prevent overflow issues
- Comprehensive input validation ensures malformed TID strings result in appropriate error messages
- The function allocates memory for the result ItemPointer using `palloc`
- Error messages follow PostgreSQL standards and include the invalid input for debugging
- The parsing logic is noted to be largely derived from the `boxin()` function implementation

## Simplified Source

```c
Datum tidin(PG_FUNCTION_ARGS) {
    char *input_string = PG_GETARG_CSTRING(0);
    Node *error_context = fcinfo->context;
    char *coordinate_pointers[NTIDARGS];
    int i;
    ItemPointer result;
    BlockNumber block_number;
    OffsetNumber offset_number;

    // Parse input string to extract block and offset coordinates
    // Look for format "(block,offset)" and store pointers to block and offset parts
    for (i = 0, p = input_string; *p && i < NTIDARGS && *p != ')'; p++) {
        if (*p == ',' || (*p == '(' && i == 0)) {
            coordinate_pointers[i++] = p + 1;
        }
    }

    // Validate that we found both block and offset parts
    if (i < NTIDARGS) {
        ereturn(error_context, (Datum) 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "tid", input_string)));
    }

    // Convert block number string to integer with validation
    unsigned long converted_value = strtoul(coordinate_pointers[0], &bad_pointer, 10);
    if (errno || *bad_pointer != ',') {
        ereturn(error_context, (Datum) 0, /* error reporting */);
    }
    block_number = (BlockNumber) converted_value;

    // Handle platform differences where unsigned long > BlockNumber
    if (converted_value != (unsigned long) block_number) {
        ereturn(error_context, (Datum) 0, /* error reporting */);
    }

    // Convert offset number string to integer with validation
    converted_value = strtoul(coordinate_pointers[1], &bad_pointer, 10);
    if (errno || *bad_pointer != ')' || converted_value > USHRT_MAX) {
        ereturn(error_context, (Datum) 0, /* error reporting */);
    }
    offset_number = (OffsetNumber) converted_value;

    // Create and return the ItemPointer result
    result = (ItemPointer) palloc(sizeof(ItemPointerData));
    ItemPointerSet(result, block_number, offset_number);

    PG_RETURN_ITEMPOINTER(result);
}
```