# intoasc

## Location
[src/interfaces/ecpg/compatlib/informix.c:672-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L672-L701)

## Overview
A compatibility wrapper function that converts an interval data structure to its ASCII string representation, providing Informix-style interval formatting functionality.

## Definition

```c
struct
{
	long		val;
	int			maxdigits;
	int			digits;
	int			remaining;
	char		sign;
	char	   *val_string;
}			value;
```
## Detailed Description
The `intoasc` function is part of PostgreSQL's ECPG Informix compatibility layer that converts an interval data structure into its ASCII string representation. It wraps the PostgreSQL native `PGTYPESinterval_to_asc` function, handling memory management and error reporting in an Informix-compatible manner.

The function allocates temporary memory for the conversion, copies the result to the provided buffer, and properly frees the temporary memory. It uses errno-based error reporting, returning the negated errno value on failure and 0 on success.

## Parameters / Member Variables
- `i`: Pointer to the interval structure to be converted to ASCII
- `str`: Output buffer where the ASCII representation of the interval will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESinterval_to_asc](../P/PGTYPESinterval_to_asc.md) (performs the actual interval-to-string conversion)
  - strcpy (copies the result string to the output buffer)
  - free (frees the temporary string allocated by PGTYPESinterval_to_asc)
- Called from (representative examples):
  - Available through ECPG_INFORMIX_EXTRA_CHARS interface
  - Used in test cases (src/interfaces/ecpg/test/expected/compat_informix-intoasc.c)

## Notes and Other Information
- Returns 0 on success, negative errno value on failure
- Part of the Informix compatibility library (`compatlib/informix.c`)
- Handles memory management automatically - the caller only needs to provide the output buffer
- The function clears errno before operation and uses it for error reporting
- Assumes the output buffer `str` is large enough to hold the converted interval string

## Simplified Source

```c
int intoasc(interval *i, char *str) {
    errno = 0;

    // Convert interval to string using PostgreSQL types library
    char *tmp = PGTYPESinterval_to_asc(i);

    // Check for conversion error
    if (!tmp)
        return -errno;

    // Copy result to output buffer and clean up
    strcpy(str, tmp);
    free(tmp);
    return 0;
}
```