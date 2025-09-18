# pg_strerror

## Location
src/port/strerror.c: 35 - 45

## Overview
A slightly cleaned-up version of the standard strerror() function that provides thread-safe error message strings for PostgreSQL.

## Definition
```c
char *pg_strerror(int errnum)
```

## Detailed Description
This function serves as PostgreSQL's standardized interface for converting error numbers to human-readable error messages. It acts as a wrapper around pg_strerror_r(), providing a simpler interface by managing the buffer internally. The function uses a static buffer to store the error message, making it suitable for single-threaded contexts or when the caller doesn't need to manage buffer allocation.

## Parameters / Member Variables
- `errnum`: The error number (errno value) to convert to a descriptive string

## Dependencies
- Functions called/Symbols referenced:
  - pg_strerror_r
  - PG_STRERROR_R_BUFLEN (buffer size constant)
- Called from (representative examples):
  - Various error handling contexts throughout PostgreSQL (referenced via printf at src/include/port.h:251)

## Notes and Other Information
- Uses a static buffer of size PG_STRERROR_R_BUFLEN, so the returned pointer is valid until the next call to this function
- Not thread-safe due to the static buffer usage
- For thread-safe operations, use pg_strerror_r() directly
- Located in src/port/strerror.c:35-45