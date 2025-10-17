# pg_indexam_progress_phasename

## Location
[src/backend/utils/adt/amutils.c:451-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L451-L467)

## Overview
This function returns the human-readable name of a specific progress phase for index building operations within a given access method.

## Definition
```c
Datum pg_indexam_progress_phasename(PG_FUNCTION_ARGS)
```

## Detailed Description
`pg_indexam_progress_phasename` is a PostgreSQL system function that provides access to the progress reporting mechanism of index access methods. It retrieves the descriptive name of a specific phase in the index building process for a given access method. This function is part of PostgreSQL's progress reporting infrastructure, which allows users to monitor the progress of long-running index operations.

The function first retrieves the access method routine structure using the provided access method OID. If the access method supports progress phase naming (via the `ambuildphasename` callback), it calls that callback with the specified phase number to get the phase name. If the access method doesn't support phase naming or the phase number is invalid, the function returns NULL.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Oid amoid): The OID of the access method to query
- `PG_FUNCTION_ARGS[1]` (int32 phasenum): The phase number for which to retrieve the name

## Dependencies
- Functions called/Symbols referenced:
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - CStringGetTextDatum
  - PG_RETURN_DATUM
- Types used:
  - [IndexAmRoutine](../I/IndexAmRoutine.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL interface)

## Notes and Other Information
- Returns NULL if the access method OID is invalid or if the access method does not support progress phase naming
- Returns NULL if the specified phase number is not recognized by the access method
- The phase names returned are access method specific and provide human-readable descriptions of index building stages
- This function is primarily used by monitoring tools and progress reporting systems to provide meaningful feedback during index operations
- The `ambuildphasename` callback in the access method routine is optional; not all access methods implement progress reporting
- Phase numbers are access method specific and typically correspond to different stages of the index building process (e.g., "scanning table", "sorting tuples", "writing index")

## Simplified Source

```c
Datum pg_indexam_progress_phasename(PG_FUNCTION_ARGS) {
    Oid amoid = PG_GETARG_OID(0);
    int32 phasenum = PG_GETARG_INT32(1);

    // Get the access method routine for the given OID
    IndexAmRoutine *routine = GetIndexAmRoutineByAmId(amoid, true);

    // Return NULL if AM doesn't support progress phase naming
    if (routine == NULL || !routine->ambuildphasename)
        PG_RETURN_NULL();

    // Get the phase name from the access method
    char *name = routine->ambuildphasename(phasenum);
    if (!name)
        PG_RETURN_NULL();

    // Convert C string to PostgreSQL text datum and return
    PG_RETURN_DATUM(CStringGetTextDatum(name));
}
```