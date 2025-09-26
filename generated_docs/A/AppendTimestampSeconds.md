# AppendTimestampSeconds

## Location
[src/backend/utils/adt/datetime.c:511-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L511-L521)

## Overview
AppendTimestampSeconds is a specialized wrapper function that formats seconds and fractional seconds for timestamp output with maximum timestamp precision and zero padding enabled.

## Definition

```c
static char *
AppendTimestampSeconds(char *cp, struct pg_tm *tm, fsec_t fsec)
```
## Detailed Description
AppendTimestampSeconds is a convenience function that provides a timestamp-specific interface to the more general AppendSeconds function. It extracts the seconds field from a pg_tm structure and formats it along with fractional seconds using fixed parameters optimized for timestamp display:

- Uses MAX_TIMESTAMP_PRECISION for fractional seconds precision
- Always enables zero padding for consistent timestamp formatting
- Leverages the full functionality of AppendSeconds while providing a simplified interface for timestamp use cases

This function serves as an abstraction layer that encapsulates timestamp-specific formatting requirements.

## Parameters / Member Variables
- `cp`: Pointer to the current position in the output string buffer
- `tm`: Pointer to pg_tm structure containing broken-down time components
- `fsec`: Fractional seconds as fsec_t (microseconds)

## Dependencies
- Functions called/Symbols referenced:
  - [AppendSeconds](AppendSeconds.md) (core formatting function)
  - MAX_TIMESTAMP_PRECISION (constant defining maximum precision)
  - [pg_tm](../p/pg_tm.md) (PostgreSQL time structure)
  - fsec_t (fractional seconds type)
- Called from (representative examples):
  - [EncodeDateTime](../E/EncodeDateTime.md) (multiple locations for various timestamp formats)

## Notes and Other Information
- This is a thin wrapper around AppendSeconds with timestamp-optimized defaults
- Always uses zero padding (fillzeros=true) to ensure consistent timestamp formatting
- Uses MAX_TIMESTAMP_PRECISION to provide maximum available precision for timestamps
- Like AppendSeconds, it does not NUL-terminate the result
- Part of PostgreSQL's timestamp encoding infrastructure, specifically used in EncodeDateTime for various timestamp format outputs