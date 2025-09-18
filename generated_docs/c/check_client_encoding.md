# check_client_encoding

## Location
src/backend/commands/variable.c: 680 - 755

## Overview
A GUC check hook function that validates and canonicalizes client encoding values when the client_encoding parameter is being set in PostgreSQL.

## Definition
```c
bool check_client_encoding(char **newval, void **extra, GucSource source)
```

## Detailed Description
The `check_client_encoding` function serves as a validation and preparation hook for PostgreSQL's client_encoding configuration parameter. It is called whenever the system attempts to change the client_encoding setting (via SET CLIENT_ENCODING, postgresql.conf, etc.).

The function performs several critical operations:
1. **Encoding Validation**: Uses `pg_valid_client_encoding` to verify that the provided encoding name is valid and supported
2. **Canonicalization**: Converts the encoding name to its canonical form using `pg_encoding_to_char`, eliminating aliases and case variations
3. **Conversion Support Check**: Calls `PrepareClientEncoding` to ensure that conversion procedures between the client encoding and database encoding are available
4. **Transaction State Handling**: Provides different error messages depending on whether the change is attempted within a transaction or during configuration reload
5. **JDBC Compatibility**: Maintains a workaround for pre-9.1 JDBC drivers that expect "UNICODE" to remain unchanged
6. **Memory Management**: Allocates memory for the encoding ID and stores it in the extra parameter for use by the assign hook

## Parameters / Member Variables
- `newval`: Pointer to the proposed new encoding name string; may be modified to canonical form
- `extra`: Pointer to store additional data (encoding ID) for the assign hook
- `source`: The source of the configuration change (e.g., SET command, config file, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_valid_client_encoding](../p/pg_valid_client_encoding.md)
  - pg_encoding_to_char
  - [PrepareClientEncoding](../P/PrepareClientEncoding.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - GUC_check_errcode
  - GUC_check_errdetail
  - [GetDatabaseEncodingName](../G/GetDatabaseEncodingName.md)
  - [guc_free](../g/guc_free.md)
  - [guc_strdup](../g/guc_strdup.md)
  - [guc_malloc](../g/guc_malloc.md)
- Called from (representative examples):
  - GUC system when processing SET CLIENT_ENCODING commands
  - Configuration file processing during server startup or SIGHUP

## Notes and Other Information
- This function is part of a triplet of GUC hooks for client_encoding: check_client_encoding, assign_client_encoding, and show_client_encoding
- Contains a compatibility workaround for pre-9.1 JDBC drivers that prevents canonicalization of "UNICODE"
- The function can fail if conversion procedures are not available, particularly during configuration reloads outside transactions
- Memory allocated for the encoding ID in *extra is freed automatically by the GUC system
- Located in src/backend/commands/variable.c as part of the encoding-related GUC functions
- The canonical encoding name replacement helps maintain consistency across the system