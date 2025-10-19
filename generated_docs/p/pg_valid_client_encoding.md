# pg_valid_client_encoding

## Location
[src/common/encnames.c:485-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L485-L498)

## Overview
Validates whether a given encoding name is a valid client-side character encoding in PostgreSQL.

## Definition
int pg_valid_client_encoding(const char *name)

## Detailed Description
This function validates an encoding name string to determine if it represents a valid client-side character encoding. It performs a two-step validation process: first converting the encoding name to an internal encoding identifier using pg_char_to_encoding, then checking if the resulting encoding is valid for frontend (client) use with the PG_VALID_FE_ENCODING macro. Client encodings are those that can be used by PostgreSQL clients to communicate with the server.

## Parameters / Member Variables
- name: String containing the name of the character encoding to validate

## Dependencies
- Functions called/Symbols referenced:
  - [pg_char_to_encoding](pg_char_to_encoding.md) (converts encoding name string to internal encoding ID)
  - PG_VALID_FE_ENCODING (macro for validating frontend/client encodings)
- Called from (representative examples):
  - [check_client_encoding](../c/check_client_encoding.md) (src/backend/commands/variable.c:686)

## Notes and Other Information
- Returns the encoding ID (positive integer) if valid, or -1 if invalid
- Frontend encodings are a subset of all PostgreSQL encodings that are suitable for client use
- Used for validating client_encoding parameter settings
- Located in src/common/encnames.c:485-498

## Simplified Source

```c
int pg_valid_client_encoding(const char *name) {
    // Convert encoding name to internal encoding ID
    int enc = pg_char_to_encoding(name);
    if (enc < 0)
        return -1;  // Unknown encoding name

    // Check if this encoding is valid for client (frontend) use
    if (!PG_VALID_FE_ENCODING(enc))
        return -1;  // Not a valid client encoding

    return enc;  // Return the valid encoding ID
}
```