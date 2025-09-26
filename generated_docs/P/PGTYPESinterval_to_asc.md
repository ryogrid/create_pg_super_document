# PGTYPESinterval_to_asc

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:1062-1081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L1062-L1081)

## Overview
Converts an interval data structure to its string representation in PostgreSQL format.

## Definition
char *PGTYPESinterval_to_asc(interval *span)

## Detailed Description
This function converts an internal interval data structure into a human-readable string representation. It first decomposes the interval into its component parts (years, months, days, hours, minutes, seconds, and fractional seconds) using interval2tm, then formats these components into a standard PostgreSQL interval string using EncodeInterval with INTSTYLE_POSTGRES_VERBOSE style. The resulting string is dynamically allocated and must be freed by the caller.

The function provides error handling by checking the validity of the interval conversion and setting errno appropriately if the interval cannot be processed. The output format follows PostgreSQL's verbose interval representation style.

## Parameters / Member Variables
- span: Pointer to the interval structure to be converted to string format. This should be a valid interval that was previously created or initialized.

## Dependencies
- Functions called/Symbols referenced:
  - [interval2tm](../i/interval2tm.md) (converts interval to tm structure)
  - [EncodeInterval](../E/EncodeInterval.md) (formats tm structure to string)
  - [pgtypes_strdup](../p/pgtypes_strdup.md) (duplicates string)
- Called from (representative examples):
  - [intoasc](../i/intoasc.md) (Informix compatibility function)
  - [ecpg_store_input](../e/ecpg_store_input.md) (ECPG input storage)
  - [main](../m/main.md) (in various test programs)
  - Client applications needing string representation of intervals

## Notes and Other Information
- Returns NULL on error and sets errno to PGTYPES_INTVL_BAD_INTERVAL
- The returned string is dynamically allocated and must be freed by the caller using free()
- Uses PostgreSQL verbose interval style for consistent formatting
- Output string length is limited to MAXDATELEN + 1 characters
- Part of the ECPG pgtypes library providing client-side PostgreSQL data type support
- Complementary function to PGTYPESinterval_from_asc for bidirectional conversion