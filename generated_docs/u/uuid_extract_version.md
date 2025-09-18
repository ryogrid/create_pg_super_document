# uuid_extract_version

## Location
src/backend/utils/adt/uuid.c: 479 - 491

## Overview
Extracts the version number from a UUID value, returning null for non-RFC 4122 variant UUIDs.

## Definition


## Detailed Description
This function extracts the version field from a UUID (Universally Unique Identifier) according to RFC 4122 specification. The function first validates that the input UUID follows the RFC 4122 variant by checking specific bits in the UUID structure. If the UUID is not RFC 4122 compliant, the function returns NULL. Otherwise, it extracts and returns the 4-bit version field from the UUID.

The UUID version indicates the algorithm used to generate the UUID:
- Version 1: Time-based UUID
- Version 2: DCE Security UUID  
- Version 3: Name-based UUID using MD5
- Version 4: Random UUID
- Version 5: Name-based UUID using SHA-1

The function is implemented as a PostgreSQL internal function and is exposed as a SQL function that takes a UUID parameter and returns a smallint (int2) value.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (pg_uuid_t*): Input UUID value from which to extract the version

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_UUID_P: Macro to extract UUID argument from function call
  - PG_RETURN_NULL: Macro to return NULL value
  - PG_RETURN_UINT16: Macro to return 16-bit unsigned integer value
  - pg_uuid_t: PostgreSQL UUID data type structure
- Called from (representative examples):
  - No direct C function callers (exposed as SQL function only)

## Notes and Other Information
- The function is marked as 'proleakproof' in the system catalog, indicating it does not leak information about its arguments
- Returns NULL for UUIDs that don't conform to RFC 4122 variant (checked via bits 6-7 of byte 8)
- The version is extracted from the upper 4 bits of byte 6 in the UUID data array
- Introduced in PostgreSQL 17 as part of enhanced UUID functionality
- Used in regression tests to validate UUID version extraction for different UUID types
- The SQL function signature is: uuid_extract_version(uuid) → smallint