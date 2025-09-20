# enum_out

## Location
[src/backend/utils/adt/enum.c:155-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L155-L178)

## Overview
Converts an internal enum OID value to its string representation for display and output purposes.

## Definition

```c
Datum
enum_out(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the output conversion for PostgreSQL enum types, transforming internal OID values back into their corresponding human-readable enum labels. It serves as the standard output function for enum types, handling the reverse conversion of what enum_in performs.

The function takes an enum's internal OID representation and looks up the corresponding enum label in the system catalog. It validates that the OID corresponds to a valid enum value and returns a newly allocated string containing the enum label. Unlike enum_in, this function does not need to perform safety checks on uncommitted values since it's converting from internal representation to display format.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The internal OID representation of the enum value to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_enum (referenced twice for struct access)
  - PG_RETURN_CSTRING
- Called from (representative examples):
  - [anyenum_out](../a/anyenum_out.md)

## Notes and Other Information
- This is a PostgreSQL I/O function that follows the standard Datum-returning pattern for output functions
- Uses SearchSysCache1 with ENUMOID cache for efficient enum label lookup
- Returns a newly allocated string using pstrdup, which the caller is responsible for managing
- Does not perform check_safe_enum_use validation since output conversion doesn't create the same safety concerns as input conversion
- Provides specific error messages for invalid internal enum OID values
- Part of the basic I/O support for enum types, complementing enum_in
- The function is also used by anyenum_out for pseudotype handling