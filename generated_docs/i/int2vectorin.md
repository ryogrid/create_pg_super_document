# int2vectorin

## Location
[src/backend/utils/adt/int.c:141-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L141-L206)

## Overview
Converts a string representation of space-separated smallint values ("num num ...") into the internal PostgreSQL int2vector data type.

## Definition

```c
Datum
int2vectorin(PG_FUNCTION_ARGS)
```
## Detailed Description
This function parses a textual representation of a vector of smallint (int2) values and converts it into PostgreSQL's internal int2vector format. The input string should contain space-separated integer values within the smallint range (-32768 to 32767). The function dynamically allocates memory for the result vector, starting with an initial guess of 32 elements and doubling the allocation when needed. It performs comprehensive error checking for invalid syntax, out-of-range values, and improper formatting.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input C-string containing space-separated smallint values
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  -  (data type)
  -  (macro for size calculation)
  -  (memory allocation)
  -  (memory reallocation)
  -  (error return with context)
  -  (set variable size)
  -  (string to long conversion)
- Called from (representative examples):
  - PostgreSQL type input/output system
  - SQL parsing and execution engine

## Notes and Other Information
- The function starts with an arbitrary initial allocation of 32 elements and doubles when needed for efficiency
- Supports soft error handling through the escontext parameter
- Sets standard array metadata: ndim=1, dataoffset=0, elemtype=INT2OID, lbound1=0
- Performs strict validation of input format and numeric ranges
- Returns a properly formatted int2vector suitable for internal PostgreSQL operations

## Simplified Source

```c
// Simplified version of int2vectorin
Datum int2vectorin(PG_FUNCTION_ARGS) {
    char *input_string = PG_GETARG_CSTRING(0);
    Node *error_context = fcinfo->context;
    int2vector *result;
    int allocated_size = 32;  // Initial allocation guess
    int element_count = 0;

    // Allocate initial memory for result vector
    result = (int2vector *) palloc0(Int2VectorSize(allocated_size));

    // Parse space-separated integers from input string
    while (true) {
        long parsed_value;
        char *end_pointer;

        // Skip whitespace
        while (*input_string && isspace(*input_string))
            input_string++;
        if (*input_string == '\0')
            break;

        // Expand allocation if needed
        if (element_count >= allocated_size) {
            allocated_size *= 2;
            result = (int2vector *) repalloc(result, Int2VectorSize(allocated_size));
        }

        // Parse integer value with error checking
        parsed_value = strtol(input_string, &end_pointer, 10);

        // Validate parsing success and range
        if (input_string == end_pointer || parsed_value < SHRT_MIN || parsed_value > SHRT_MAX) {
            ereturn(error_context, (Datum) 0, /* error details */);
        }

        // Store value and advance to next token
        result->values[element_count] = parsed_value;
        input_string = end_pointer;
        element_count++;
    }

    // Set final vector metadata
    SET_VARSIZE(result, Int2VectorSize(element_count));
    result->ndim = 1;
    result->dataoffset = 0;
    result->elemtype = INT2OID;
    result->dim1 = element_count;
    result->lbound1 = 0;

    PG_RETURN_POINTER(result);
}
```

Key simplifications made:
- Removed detailed error handling messages for clarity
- Used more descriptive variable names
- Consolidated error checking logic
- Added high-level comments explaining each major step
- Focused on the main parsing and allocation algorithm