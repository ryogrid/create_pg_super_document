# PGTYPESinterval_to_asc

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 1062 - 1081

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
  - interval2tm (converts interval to tm structure)
  - EncodeInterval (formats tm structure to string)
  - pgtypes_strdup (duplicates string)
- Called from (representative examples):
  - intoasc (Informix compatibility function)
  - ecpg_store_input (ECPG input storage)
  - main (in various test programs)
  - Client applications needing string representation of intervals

## Notes and Other Information
- Returns NULL on error and sets errno to PGTYPES_INTVL_BAD_INTERVAL
- The returned string is dynamically allocated and must be freed by the caller using free()
- Uses PostgreSQL verbose interval style for consistent formatting
- Output string length is limited to MAXDATELEN + 1 characters
- Part of the ECPG pgtypes library providing client-side PostgreSQL data type support
- Complementary function to PGTYPESinterval_from_asc for bidirectional conversion