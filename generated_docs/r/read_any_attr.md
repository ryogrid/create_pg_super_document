# read_any_attr

## Location
src/backend/libpq/auth-scram.c: 841 - 898

## Overview
A parser function that extracts the next attribute-value pair from a SCRAM authentication message string according to the SCRAM protocol specification.

## Definition


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