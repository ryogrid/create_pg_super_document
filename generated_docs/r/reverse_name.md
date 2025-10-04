# reverse_name

## Location
[src/test/regress/regress.c:233-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L233-L253)

## Overview
This function reverses the character order of a given string, creating a new string with characters in reverse order while respecting PostgreSQL's name length limitations.

## Definition
```c
Datum reverse_name(PG_FUNCTION_ARGS)
```

## Detailed Description
reverse_name is a PostgreSQL function that takes a C-string as input and returns a new string with the characters in reverse order. The function is designed to work within PostgreSQL's naming constraints, using NAMEDATALEN to limit the maximum length of the processed string. It allocates memory for the new string using palloc0, determines the actual length of the input string (up to NAMEDATALEN), and then copies characters from the input string to the output string in reverse order.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that encapsulates:
  - `string`: A C-string (char*) to be reversed (first argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extracts C-string from function arguments)
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - NAMEDATALEN (PostgreSQL constant defining maximum name length)
  - PG_RETURN_CSTRING (returns C-string result)
- Called from (representative examples):
  - [pt_in_widget](../p/pt_in_widget.md) (appears in reference context)

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite, demonstrating string manipulation techniques
- Uses palloc0 for memory allocation, which is PostgreSQL's memory management system that ensures proper cleanup
- Respects NAMEDATALEN limit to prevent buffer overflows and maintain compatibility with PostgreSQL's internal naming conventions
- The algorithm first determines the actual string length (up to NAMEDATALEN), then reverses the characters by copying from end to beginning
- Located in src/test/regress/regress.c, indicating it's primarily for testing purposes
- Handles edge cases where strings may be exactly NAMEDATALEN characters long

## Simplified Source

```c
Datum reverse_name(PG_FUNCTION_ARGS) {
    // Get input string and allocate memory for reversed string
    char *string = PG_GETARG_CSTRING(0);
    char *new_string = palloc0(NAMEDATALEN);

    // Find the actual length of the string (up to NAMEDATALEN)
    int i;
    for (i = 0; i < NAMEDATALEN && string[i]; ++i)
        ;
    if (i == NAMEDATALEN || !string[i])
        --i;
    int len = i;

    // Copy characters in reverse order
    for (; i >= 0; --i)
        new_string[len - i] = string[i];

    PG_RETURN_CSTRING(new_string);
}
```