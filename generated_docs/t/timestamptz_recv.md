# timestamptz_recv

## Location
src/backend/utils/adt/timestamp.c: 813 - 846

## Overview
Converts external binary format data to PostgreSQL's internal timestamptz representation during binary input operations.

## Definition


## Detailed Description
The  function serves as the binary input function for the timestamptz data type. It reads a 64-bit integer from a StringInfo buffer representing a timestamp with timezone in PostgreSQL's internal binary format, validates the value, and applies any necessary typmod adjustments. This function is part of PostgreSQL's binary I/O protocol, used when receiving timestamptz values in binary format from clients or during binary data transfer operations.

The function performs range checking to ensure the timestamp value is valid and can be properly represented, throwing an error for out-of-range values. It also handles special timestamp values (infinity, -infinity) and applies precision adjustments based on the typmod parameter.

## Parameters / Member Variables
- : StringInfo containing the binary data to be converted (from )
- : Type element OID (from , currently unused)
- : Type modifier for precision control (from )

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract StringInfo buffer argument
  - : Macro to extract typmod argument
  - : Extracts 64-bit integer from binary message buffer
  - : Macro to check for infinite timestamps
  - : Validates timestamp by converting to broken-down time
  - : Validates timestamp range
  - : Applies precision adjustments based on typmod
  - : Macro to return the timestamptz result
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:813-846
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- Performs comprehensive validation including range checking and typmod adjustment
- Handles both finite and infinite timestamp values
- The typelem parameter is marked as NOT_USED but preserved for function signature compatibility
- Throws ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error for invalid timestamp values
- Used by PostgreSQL's type system infrastructure rather than being called directly by user code