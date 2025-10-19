# check_encoding_conversion_args

## Location
[src/backend/utils/mb/mbutils.c:1669-1697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1669-L1697)

## Overview
Validates the arguments passed to encoding conversion functions to ensure they have valid encoding IDs and match expected values.

## Definition
```c
void check_encoding_conversion_args(int src_encoding, int dest_encoding, int len, int expected_src_encoding, int expected_dest_encoding)
```

## Detailed Description
This function performs validation of parameters commonly used in encoding conversion functions. It verifies that both source and destination encoding IDs are valid using PG_VALID_ENCODING, checks that they match expected values when specified, and ensures the length parameter is non-negative. The function is designed for internal use by conversion functions to catch programming errors early.

The function uses elog rather than ereport for error reporting, as these are considered internal programming errors rather than user-facing issues. Expected encoding arguments can be set to -1 to skip the matching check, allowing callers to perform their own validation logic.

## Parameters / Member Variables
- `src_encoding`: Source encoding ID to validate
- `dest_encoding`: Destination encoding ID to validate  
- `len`: Length parameter that must be non-negative
- `expected_src_encoding`: Expected source encoding ID, or -1 to skip validation
- `expected_dest_encoding`: Expected destination encoding ID, or -1 to skip validation

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (macro for encoding validation)
  - elog (PostgreSQL error logging function)
  - pg_enc2name_tbl (encoding name lookup table)
- Called from (representative examples):
  - CHECK_ENCODING_CONVERSION_ARGS (macro wrapper)

## Notes and Other Information
- Uses elog instead of ereport since these are internal programming errors, not user errors
- Expected encoding parameters can be -1 to indicate that the caller will perform its own validation
- The function trusts that expected encoding arguments are valid but validates the actual encoding IDs
- Provides detailed error messages including encoding names when mismatches occur
- Validates that the length parameter is non-negative, preventing buffer underflow issues
- Primarily used through the CHECK_ENCODING_CONVERSION_ARGS macro for convenience

## Simplified Source

```c
void
check_encoding_conversion_args(int src_encoding,
                               int dest_encoding,
                               int len,
                               int expected_src_encoding,
                               int expected_dest_encoding)
{
    // Validate source encoding ID
    if (!PG_VALID_ENCODING(src_encoding))
        elog(ERROR, "invalid source encoding ID: %d", src_encoding);

    // Check if source encoding matches expected (if specified)
    if (src_encoding != expected_src_encoding && expected_src_encoding >= 0)
        elog(ERROR, "expected source encoding \"%s\", but got \"%s\"",
             pg_enc2name_tbl[expected_src_encoding].name,
             pg_enc2name_tbl[src_encoding].name);

    // Validate destination encoding ID
    if (!PG_VALID_ENCODING(dest_encoding))
        elog(ERROR, "invalid destination encoding ID: %d", dest_encoding);

    // Check if destination encoding matches expected (if specified)
    if (dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0)
        elog(ERROR, "expected destination encoding \"%s\", but got \"%s\"",
             pg_enc2name_tbl[expected_dest_encoding].name,
             pg_enc2name_tbl[dest_encoding].name);

    // Ensure length is non-negative
    if (len < 0)
        elog(ERROR, "encoding conversion length must not be negative");
}
```