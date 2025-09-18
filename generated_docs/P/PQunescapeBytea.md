# PQunescapeBytea

## Location
[src/interfaces/libpq/fe-exec.c:4555-4664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4555-L4664)

## Overview
PQunescapeBytea converts escaped string representations of bytea data back into binary format, supporting both hex and traditional octal escape encodings.

## Definition


## Detailed Description
PQunescapeBytea is a libpq client library function that performs the reverse operation of PQescapeBytea. It converts null-terminated string representations of bytea data back into binary format. The function handles two encoding formats:

1. **Hex format**: Strings starting with '\x' followed by hexadecimal digit pairs
2. **Traditional escape format**: Strings using octal escape sequences and backslash doubling

For hex format (\x followed by hex digits):
- Each pair of hex digits represents one byte
- Whitespace between hex pairs is silently ignored (compatible with byteain)
- Bad input characters are silently skipped

For traditional escape format, the following transformations are reversed:
- \\ becomes \ (ASCII 92)
- \ooo becomes a byte with octal value ooo (where ooo is a 3-digit octal number)
- \x becomes x (for any other character x not matching above patterns)

The function automatically detects the format by checking if the string starts with '\x'.

## Parameters / Member Variables
- : Pointer to the null-terminated escaped string to be converted
- : Output parameter - pointer to size_t where the length of the result buffer will be stored

## Dependencies
- Functions called/Symbols referenced:
  - malloc (memory allocation for result buffer)
  - [get_hex](../g/get_hex.md) (converts hex character to numeric value)
  - realloc (shrinks buffer to optimal size)
  - ISFIRSTOCTDIGIT (macro: checks if character is valid first octal digit 0-3)
  - ISOCTDIGIT (macro: checks if character is valid octal digit 0-7) 
  - OCTVAL (macro: converts octal character to numeric value)
- Called from (representative examples):
  - Referenced in libpq-fe.h header file declarations

## Notes and Other Information
- Returns NULL if input is NULL or memory allocation fails
- The function allocates memory using malloc() - caller must free the result using PQfreemem() or free()
- The buffer is shrunk to the minimum required size using realloc() for memory efficiency
- Handles malformed input gracefully by silently ignoring bad characters in hex mode
- For traditional escape mode, unrecognized escape sequences after '\' are ignored (the character after '\' will be treated as literal data)
- Corner case: A trailing '\' at the end of input is simply discarded
- The returned buffer length does not include a null terminator (this is binary data)
- Uses defensive malloc(1) instead of malloc(0) to avoid unportable behavior with zero-length inputs