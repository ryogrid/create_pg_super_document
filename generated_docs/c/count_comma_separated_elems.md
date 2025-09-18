# count_comma_separated_elems

## Location
[src/interfaces/libpq/fe-connect.c:1034-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L1034-L1057)

## Overview
Counts the number of elements in a simple comma-separated string by counting the comma delimiters plus one.

## Definition
```c
static int count_comma_separated_elems(const char *input)
```

## Detailed Description
This utility function provides a simple way to determine how many elements are present in a comma-separated list string. It works by iterating through the input string character by character and counting comma characters, then adding one to account for the fact that n commas separate n+1 elements. The function assumes the input is a well-formed comma-separated list and does not handle escaped commas or quoted sections.

## Parameters / Member Variables
- `input`: A null-terminated string containing comma-separated elements to count

## Dependencies
- Functions called/Symbols referenced:
  - (none - uses only basic C string operations)
- Called from (representative examples):
  - pqConnectOptions2 (multiple calls for counting hostaddr, host, port, and other connection parameters)

## Notes and Other Information
- Returns the count of elements as an integer
- Always returns at least 1 for non-empty strings (assumes at least one element)
- Does not validate the format of the comma-separated list
- Does not handle escaped commas or quoted values that might contain commas
- Simple implementation optimized for basic comma-separated parameter lists in libpq
- Location: src/interfaces/libpq/fe-connect.c:1034-1057