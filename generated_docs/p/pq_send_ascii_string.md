# pq_send_ascii_string

## Location
[src/backend/libpq/pqformat.c:227-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L227-L251)

## Overview
Appends a null-terminated text string to a StringInfo buffer without encoding conversion, replacing non-ASCII characters with question marks for safe error message transmission.

## Definition

```c
union
	{
		float4		f;
		uint32		i;
	}			swap;
```
## Detailed Description
The pq_send_ascii_string function is designed as a fallback mechanism for sending strings to clients when normal encoding conversion is problematic or unavailable. Unlike pq_sendstring, this function intentionally bypasses character set conversion entirely. Instead, it ensures safety by scanning each character in the input string and replacing any non-7-bit-ASCII characters (characters with the high bit set) with question marks ('?').

This function is specifically used in error handling scenarios where PostgreSQL is having trouble sending error messages to the client through the normal localization and encoding conversion process. The function provides a guaranteed way to send a comprehensible message even when the regular communication mechanisms fail, ensuring that clients receive something readable rather than potentially corrupted or malformed data.

## Parameters / Member Variables
- : StringInfo buffer to append the ASCII-safe string to
- : Null-terminated input string to be processed and appended

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - appendStringInfoCharMacro (macro to append single character to buffer)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (appends null terminator)

- Called from (representative examples):
  - [err_sendstring](../e/err_sendstring.md) (error message handling when encoding conversion fails)

## Notes and Other Information
- This function is specifically designed for error recovery scenarios
- Non-ASCII characters are replaced with '?' to ensure 7-bit ASCII compatibility
- The function processes strings character by character for safety
- Input string must be null-terminated, and output is also null-terminated
- This is a more conservative alternative to pq_sendstring when encoding issues arise
- The function ensures that no badly encoded strings are sent to the client
- Part of PostgreSQL's robust error handling system that prioritizes message delivery over perfect formatting