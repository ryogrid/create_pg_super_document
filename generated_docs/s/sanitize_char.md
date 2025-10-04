# sanitize_char

## Location
[src/backend/libpq/auth-scram.c:793-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L793-L812)

## Overview
Converts an arbitrary byte to a safe printable representation for use in error messages.

## Definition
```c
static char *sanitize_char(char c)
```

## Detailed Description
This utility function converts any character to a safe string representation that can be included in error messages without risk of terminal control character injection or other display issues. If the character is a printable ASCII character (0x21-0x7E), it returns the character enclosed in single quotes (e.g., "'a'"). For non-printable characters, it returns the hexadecimal representation (e.g., "0x0a").

The function uses a static buffer to store the result, so consecutive calls will overwrite previous results. This design is suitable for its primary use case in error message formatting where the result is consumed immediately.

## Parameters / Member Variables
- `c`: The character to be sanitized for safe display

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
- Called from (representative examples):
  - scram_state
  - [read_attr_value](../r/read_attr_value.md)
  - [read_any_attr](../r/read_any_attr.md)
  - [read_client_first_message](../r/read_client_first_message.md)

## Notes and Other Information
- This is a static function, only accessible within auth-scram.c
- Uses a static buffer of size 5 bytes (sufficient for "'x'" or "0xNN")
- The static buffer means the function is not thread-safe for concurrent access
- Printable range 0x21-0x7E excludes control characters and space
- Used primarily in SCRAM protocol error messages to safely display potentially malicious input
- Returns a pointer to the static buffer, so the result must be used immediately or copied
- Part of defensive programming practices to prevent error message injection attacks

## Simplified Source

```c
static char *sanitize_char(char c) {
    static char buf[5];

    // If printable ASCII character, show in quotes
    if (c >= 0x21 && c <= 0x7E)
        snprintf(buf, sizeof(buf), "'%c'", c);
    else
        // Otherwise show as hex value
        snprintf(buf, sizeof(buf), "0x%02x", (unsigned char) c);

    return buf;
}
```