# intoasc

## Location
src/interfaces/ecpg/compatlib/informix.c: 672 - 701

## Overview
A compatibility wrapper function that converts an interval data structure to its ASCII string representation, providing Informix-style interval formatting functionality.

## Definition


## Detailed Description
The `intoasc` function is part of PostgreSQL's ECPG Informix compatibility layer that converts an interval data structure into its ASCII string representation. It wraps the PostgreSQL native `PGTYPESinterval_to_asc` function, handling memory management and error reporting in an Informix-compatible manner.

The function allocates temporary memory for the conversion, copies the result to the provided buffer, and properly frees the temporary memory. It uses errno-based error reporting, returning the negated errno value on failure and 0 on success.

## Parameters / Member Variables
- `i`: Pointer to the interval structure to be converted to ASCII
- `str`: Output buffer where the ASCII representation of the interval will be stored

## Dependencies
- Functions called/Symbols referenced:
  - PGTYPESinterval_to_asc (performs the actual interval-to-string conversion)
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