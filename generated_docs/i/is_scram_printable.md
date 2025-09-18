# is_scram_printable

## Location
[src/backend/libpq/auth-scram.c:765-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L765-L792)

## Overview
Validates that a string contains only characters that are printable according to the SCRAM specification (RFC 5802).

## Definition
```c
static bool is_scram_printable(char *p)
```

## Detailed Description
This function checks whether a given string conforms to the SCRAM (Salted Challenge Response Authentication Mechanism) specification's definition of "printable" characters. According to RFC 5802, printable characters are defined as ASCII characters in the ranges 0x21-2B and 0x2D-7E, which includes all printable ASCII characters except the comma (0x2C).

The function iterates through each character in the string and returns false immediately if any character falls outside the allowed ranges or is a comma. It returns true only if all characters in the string are SCRAM-printable.

## Parameters / Member Variables
- `p`: Null-terminated string to be validated for SCRAM printability

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic C operations)
- Called from (representative examples):
  - scram_state
  - [read_client_first_message](../r/read_client_first_message.md)

## Notes and Other Information
- This is a static function, only accessible within auth-scram.c
- Implements the printable character definition from RFC 5802 Section 5.1
- Excludes comma (0x2C) which is used as a delimiter in SCRAM messages
- Range 0x21-2B covers: ! " # $ % & ' ( ) * +
- Range 0x2D-7E covers: - . / 0-9 : ; < = > ? @ A-Z [ \ ] ^ _ ` a-z { | } ~
- Part of input validation for SCRAM authentication protocol
- Used to prevent protocol injection attacks and ensure message integrity
- Returns false for empty strings (since loop never executes and no characters are validated)