# read_any_attr

## Location
[src/backend/libpq/auth-scram.c:841-898](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L841-L898)

## Overview
A parser function that extracts the next attribute-value pair from a SCRAM authentication message string according to the SCRAM protocol specification.

## Definition

```c
static char *
read_any_attr(char **input, char *attr_p)
```
## Detailed Description
The `read_any_attr` function implements parsing logic for SCRAM (Salted Challenge Response Authentication Mechanism) messages, which follow the format "attribute=value,attribute=value,...". It advances through the input string to locate and extract the next attribute-value pair, performing strict validation according to the SCRAM protocol specification (RFC 5802).

The function validates that attributes are single alphabetic characters (A-Z, a-z) followed by an equals sign, then extracts the value portion up to the next comma delimiter or end of string. It modifies the input pointer to advance past the parsed attribute-value pair and optionally returns the attribute character through the attr_p parameter.

Error handling is comprehensive, generating specific protocol violation errors for malformed messages, invalid attribute characters, missing equals signs, or unexpected end-of-string conditions.

## Parameters / Member Variables
- `input`: Pointer to a character pointer that points to the current position in the SCRAM message string. This pointer is advanced past the parsed attribute-value pair.
- `attr_p`: Optional output parameter to receive the parsed attribute character. Can be NULL if the attribute character is not needed.

## Dependencies
- Functions called/Symbols referenced:
  - [sanitize_char](../s/sanitize_char.md) (at Line 865)
  - ereport/errcode/errmsg/errdetail (PostgreSQL error reporting system)
- Called from (representative examples):
  - [read_client_first_message](read_client_first_message.md) (at src/backend/libpq/auth-scram.c:1103)
  - [read_client_final_message](read_client_final_message.md) (at src/backend/libpq/auth-scram.c:1370)

## Notes and Other Information
- Follows SCRAM protocol specification: attr-val = ALPHA "=" value
- Modifies the input string by null-terminating attribute values (destructive parsing)
- Returns a pointer to the value portion of the attribute-value pair
- Comprehensive error reporting for protocol violations with detailed error messages
- Used during both client-first and client-final message parsing phases
- The function assumes comma-separated attribute-value pairs and handles both intermediate and final pairs

## Simplified Source

```c
static char *read_any_attr(char **input, char *attr_p) {
    char *begin = *input;
    char *end;
    char attr = *begin;

    // Check for end of string
    if (attr == '\0')
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("malformed SCRAM message"),
                       errdetail("Attribute expected, but found end of string.")));

    // Validate attribute is alphabetic (A-Z, a-z)
    if (!((attr >= 'A' && attr <= 'Z') || (attr >= 'a' && attr <= 'z')))
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("malformed SCRAM message"),
                       errdetail("Attribute expected, but found invalid character \"%s\".",
                                sanitize_char(attr))));

    // Return attribute character if requested
    if (attr_p)
        *attr_p = attr;
    begin++;

    // Expect '=' after attribute
    if (*begin != '=')
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("malformed SCRAM message"),
                       errdetail("Expected character \"=\" for attribute \"%c\".", attr)));
    begin++;

    // Find end of value (comma or end of string)
    end = begin;
    while (*end && *end != ',')
        end++;

    // Null-terminate value and advance input pointer
    if (*end) {
        *end = '\0';
        *input = end + 1;
    } else {
        *input = end;
    }

    return begin;  // Return pointer to value
}
```