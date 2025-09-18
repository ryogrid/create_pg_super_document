# show_binary_results

## Location
[src/test/examples/testlibpq3.c:60-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/examples/testlibpq3.c#L60-L114)

## Overview
A utility function that demonstrates how to process and display binary-format query results from PostgreSQL, specifically designed to handle different data types including integers, text, and binary data.

## Definition


## Detailed Description
The show_binary_results function is part of the testlibpq3 example program that demonstrates how to handle binary-format query results from PostgreSQL. This function specifically processes results from a table with three columns: an integer ('i'), text ('t'), and bytea ('b') field.

Key functionality:
- Uses PQfnumber() to dynamically determine field positions, avoiding hardcoded assumptions about column order
- Handles binary representation of different PostgreSQL data types correctly
- Converts network byte order integers (INT4) to local byte order using ntohl()
- Properly displays TEXT fields as null-terminated C strings
- Safely handles BYTEA fields which may contain embedded null bytes
- Provides detailed output showing field lengths and values for debugging/educational purposes

The function demonstrates best practices for handling binary query results, including proper byte order conversion and safe handling of binary data that may contain null bytes.

## Parameters / Member Variables
- : A PGresult pointer containing the binary-format query results to be displayed

## Dependencies
- Functions called/Symbols referenced:
  - [PQfnumber](../P/PQfnumber.md) (libpq function to get field number by name)
  - [PQntuples](../P/PQntuples.md) (libpq function to get number of tuples/rows)
  - [PQgetvalue](../P/PQgetvalue.md) (libpq function to get field value)
  - [PQgetlength](../P/PQgetlength.md) (libpq function to get field length)
  - ntohl (network to host byte order conversion)
  - printf (standard C library function)

- Called from (representative examples):
  - [main](../m/main.md) (in testlibpq3.c, called twice to demonstrate binary result handling)

## Notes and Other Information
- This is a demonstration/test function, not part of the core PostgreSQL engine
- Located in src/test/examples/testlibpq3.c as an educational example
- Demonstrates proper handling of PostgreSQL's binary wire protocol format
- Shows important concepts like network byte order conversion for integers
- Illustrates the difference between TEXT (null-terminated) and BYTEA (length-specified) handling
- The function assumes the result set contains exactly three fields named 'i', 't', and 'b'
- Uses octal escape sequences to safely display binary data that might contain non-printable characters
- Serves as a reference implementation for applications that need to handle binary-format results efficiently