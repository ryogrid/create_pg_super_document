# read_attr_value

## Location
src/backend/libpq/auth-scram.c: 729 - 764

## Overview
Reads and extracts the value of a specified attribute from a SCRAM exchange message string.

## Definition


## Detailed Description
This function parses SCRAM (Salted Challenge Response Authentication Mechanism) protocol messages to extract attribute values. SCRAM messages contain attributes in the format "attribute=value,attribute=value,...". The function validates that the expected attribute character is present at the current position, followed by an equals sign, then extracts the value portion up to the next comma or end of string.

The function modifies the input string by null-terminating the extracted value and advances the input pointer to the next attribute position. It performs strict protocol validation, generating detailed error messages if the expected attribute format is not found.

## Parameters / Member Variables
- : Pointer to a string pointer that contains the SCRAM message. The pointer is advanced past the processed attribute.
- : The expected attribute character (e.g., 'n', 'r', 's', 'i', 'c', 'p')

## Dependencies
- Functions called/Symbols referenced:
  - [sanitize_char](../s/sanitize_char.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - ERRCODE_PROTOCOL_VIOLATION
- Called from (representative examples):
  - [read_client_first_message](read_client_first_message.md)
  - [read_client_final_message](read_client_final_message.md)  
  - [read_server_first_message](read_server_first_message.md)
  - [read_server_final_message](read_server_final_message.md)

## Notes and Other Information
- This is a static function, only accessible within auth-scram.c (and fe-auth-scram.c)
- Modifies the input string by inserting null terminators
- Advances the input pointer to facilitate sequential parsing of multiple attributes
- Uses sanitize_char() for safe error message display of potentially malicious input
- Part of PostgreSQL's SCRAM authentication protocol implementation
- Returns a pointer to the extracted value string within the original input buffer
- Generates protocol violation errors with detailed context for debugging