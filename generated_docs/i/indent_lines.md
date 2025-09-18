# indent_lines

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1006-1036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1006-L1036)

## Overview
A utility function that creates a copy of the input string with all lines indented by four spaces, used for formatting output text in pg_amcheck.

## Definition


## Detailed Description
The  function takes a string as input and returns a newly allocated copy where every line is prefixed with four spaces for indentation. It uses PostgreSQL's PQExpBuffer functionality to efficiently build the indented string. The function iterates through each character of the input string, copying it to the buffer and adding four-space indentation after every newline character (except at the very end of the string).

This function is primarily used in pg_amcheck for formatting error messages and diagnostic output to make them more readable by providing consistent indentation.

## Parameters / Member Variables
- : The input string to be indented. Each line in this string will be prefixed with four spaces.

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - initPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendPQExpBufferChar
  - [pstrdup](../p/pstrdup.md)
  - termPQExpBuffer
- Called from:
  - [verify_heap_slot_handler](../v/verify_heap_slot_handler.md) (at src/bin/pg_amcheck/pg_amcheck.c:1086)
  - [verify_btree_slot_handler](../v/verify_btree_slot_handler.md) (at src/bin/pg_amcheck/pg_amcheck.c:1155)

## Notes and Other Information
- The function returns a dynamically allocated string that must be freed by the caller using pg_free()
- The indentation is exactly four spaces per line
- The function handles empty strings and strings without newlines correctly
- It's a static function, meaning it's only accessible within the pg_amcheck.c file
- The function preserves the original content while only adding formatting indentation