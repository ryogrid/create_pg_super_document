# publicationListToArray

## Location
[src/backend/commands/subscriptioncmds.c:549-578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L549-L578)

## Overview
Converts a List of publication name strings into a PostgreSQL text array suitable for storage in system catalogs or passing to SQL functions.

## Definition


## Detailed Description
This utility function transforms a List containing String nodes (publication names) into a PostgreSQL ArrayType structure represented as a Datum. The function is essential for storing publication lists in system catalogs like pg_subscription, where the publication list is stored as a text array.

The conversion process involves:
1. Creating a temporary memory context for intermediate allocations
2. Allocating a Datum array to hold the converted string values
3. Calling check_duplicates_in_publist to validate and populate the Datum array
4. Constructing a PostgreSQL text array from the Datum array
5. Cleaning up the temporary memory context

The function ensures proper memory management by using a dedicated memory context that is cleaned up after the array construction, preventing memory leaks during the conversion process.

## Parameters / Member Variables
- : List of String nodes containing publication names to convert to array format

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates temporary memory context for allocations
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)/MemoryContextDelete: Memory context management
  - [check_duplicates_in_publist](../c/check_duplicates_in_publist.md): Validates publication list and populates Datum array
  - [construct_array_builtin](../c/construct_array_builtin.md): Constructs PostgreSQL array from Datum values
  - [PointerGetDatum](../P/PointerGetDatum.md): Converts ArrayType pointer to Datum
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md): When storing publication list in pg_subscription catalog
  - [AlterSubscription](../A/AlterSubscription.md): When updating publication list during subscription modification

## Notes and Other Information
- The function creates a temporary memory context to isolate allocations during conversion
- Uses TEXTOID as the array element type since publication names are text strings
- The check_duplicates_in_publist function handles both duplicate detection and Datum array population
- Memory context cleanup ensures no memory leaks occur during the conversion process
- The resulting array is compatible with PostgreSQL's internal array representation and can be stored in system catalogs