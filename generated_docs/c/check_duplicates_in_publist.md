# check_duplicates_in_publist

## Location
[src/backend/commands/subscriptioncmds.c:2292-2331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L2292-L2331)

## Overview
Validates a list of publication names for duplicates and optionally converts them to text datums for array construction.

## Definition

```c
static void
check_duplicates_in_publist(List *publist, Datum *datums)
```
## Detailed Description
This function performs duplicate detection on a list of publication names using a nested loop algorithm to ensure each publication appears only once in the list. When duplicates are found, it immediately reports an error with the ERRCODE_DUPLICATE_OBJECT error code. Additionally, if a datums array is provided, the function converts each publication name from the list into a text datum and stores it in the array for subsequent use in PostgreSQL array construction. This dual functionality makes it useful for both validation and data preparation phases of subscription operations.

## Parameters / Member Variables
- `*publist`: List of publication names (as string Values) to validate for duplicates
- `*datums`: Optional array to store text datums converted from publication names (can be NULL)
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

## Simplified Source

```c
static void
check_duplicates_in_publist(List *publist, Datum *datums)
{
    ListCell *cell;
    int j = 0;

    // Check each publication name against all previous ones
    foreach(cell, publist)
    {
        char *name = strVal(lfirst(cell));
        ListCell *pcell;

        // Compare with all previous publications
        foreach(pcell, publist)
        {
            char *pname = strVal(lfirst(pcell));

            if (pcell == cell)
                break;  // Reached current item, stop checking

            if (strcmp(name, pname) == 0)
                ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("publication name \"%s\" used more than once",
                                pname)));
        }

        // Convert to text datum if array provided
        if (datums)
            datums[j++] = CStringGetTextDatum(name);
    }
}
```