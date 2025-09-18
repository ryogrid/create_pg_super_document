# PQsetResultAttrs

## Location
src/interfaces/libpq/fe-exec.c: 249 - 317

## Overview
Sets the column attribute descriptors for a PGresult, including column names, types, and formats, with validation to prevent overwriting existing attributes.

## Definition


## Detailed Description
PQsetResultAttrs configures the column metadata for a PGresult by copying the provided attribute descriptors. The function performs several validation checks: it ensures the result is valid (not NULL or OOM_result), prevents overwriting existing attributes, and handles no-op cases gracefully. When setting attributes, it allocates memory for the descriptors, performs a deep copy of the attribute names to ensure memory ownership, and determines the overall result format (binary vs text) based on individual column formats.

The function maintains the binary flag of the result - if any column uses text format (format == 0), the entire result is marked as non-binary. This ensures consistent format handling across all columns in the result set.

## Parameters / Member Variables
- : Target PGresult to set attributes for
- : Number of columns in the result set
- : Array of PGresAttDesc structures containing column metadata

## Dependencies
- Functions called/Symbols referenced:
  - PQresultAlloc
  - pqResultStrdup
  - memcpy
- Called from (representative examples):
  - PQcopyResult

## Notes and Other Information
- Returns false (0) on failure, true (non-zero) on success
- Fails if attributes already exist in the result (cannot overwrite)
- Gracefully handles NULL or zero numAttributes as no-op
- Performs deep copy of attribute names to ensure proper memory management
- Automatically determines binary format based on individual column formats
- Uses result's memory context for all allocations to ensure proper cleanup