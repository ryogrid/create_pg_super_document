# sanitize_str

## Location
[src/backend/libpq/auth-scram.c:813-840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L813-L840)

## Overview
A utility function that converts arbitrary strings to printable form for safe display in error messages within PostgreSQL's SCRAM authentication system.

## Definition

```c
static char *
sanitize_str(const char *s)
```
## Detailed Description
The  function provides a safety mechanism for displaying potentially unsafe string data in error messages during SCRAM authentication. It creates a sanitized copy of the input string by replacing any non-printable characters with question marks ('?') and truncating the result to a maximum of 30 characters. This prevents potential security issues and formatting problems that could arise from displaying raw user input containing control characters, extended ASCII, or very long strings in log messages or error reports.

The function uses a static buffer to store the sanitized result, making it suitable for quick error message formatting but requiring caution in multi-threaded environments or when multiple sanitized strings need to be preserved simultaneously.

## Parameters / Member Variables
- `*s`: The input string to be sanitized. Can contain any characters including non-printable ones.
## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - uses only basic C operations)
- Called from (representative examples):
  - [read_client_first_message](../r/read_client_first_message.md) (at src/backend/libpq/auth-scram.c:1045)
  - scram_state (at src/backend/libpq/auth-scram.c:184)

## Notes and Other Information
- Returns a pointer to a static buffer, so the returned string is only valid until the next call to this function
- Only printable ASCII characters (0x21-0x7E) are preserved; all others become '?'
- Maximum output length is 30 characters plus null terminator
- Thread safety: Not thread-safe due to static buffer usage
- Primarily used for security-safe logging of potentially untrusted input during authentication processes