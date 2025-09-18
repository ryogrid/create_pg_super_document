# check_duplicates_in_publist

## Location
src/backend/commands/subscriptioncmds.c: 2292 - 2331

## Overview
Validates a list of publication names for duplicates and optionally converts them to text datums for array construction.

## Definition


## Detailed Description
This function performs duplicate detection on a list of publication names using a nested loop algorithm to ensure each publication appears only once in the list. When duplicates are found, it immediately reports an error with the ERRCODE_DUPLICATE_OBJECT error code. Additionally, if a datums array is provided, the function converts each publication name from the list into a text datum and stores it in the array for subsequent use in PostgreSQL array construction. This dual functionality makes it useful for both validation and data preparation phases of subscription operations.

## Parameters / Member Variables
- : List of publication names (as string Values) to validate for duplicates
- : Optional array to store text datums converted from publication names (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strVal
  - lfirst
  - strcmp
  - ereport
  - CStringGetTextDatum
- Called from (representative examples):
  - [publicationListToArray](../p/publicationListToArray.md)
  - [merge_publications](../m/merge_publications.md)

## Notes and Other Information
- Uses a simple O(n²) nested loop algorithm for duplicate detection, which is acceptable for typical small publication lists
- The function serves dual purposes: validation and datum array preparation for PostgreSQL internal operations
- Error reporting is immediate upon finding the first duplicate, preventing further processing
- When datums parameter is provided, it assumes the caller has allocated sufficient space for all publication names
- Critical for maintaining data integrity in subscription configurations by preventing ambiguous publication references