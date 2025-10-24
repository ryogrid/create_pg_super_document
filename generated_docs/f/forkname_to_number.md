# forkname_to_number

## Location
[src/common/relpath.c:50-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/relpath.c#L50-L80)

## Overview
Converts a fork name string to its corresponding numeric fork identifier (ForkNumber), providing a lookup mechanism for PostgreSQL's relation fork naming system.

## Definition

```c
ForkNumber
forkname_to_number(const char *forkName)
```
## Detailed Description
This function performs a linear search through the  array to find a matching fork name and returns the corresponding . PostgreSQL uses different "forks" to store various types of data for relations - the main data, free space map (fsm), visibility map (vm), and initialization fork (init). The function handles both backend and frontend contexts differently: in backend mode, it throws an error for invalid fork names, while in frontend mode, it simply returns .

## Parameters / Member Variables
- `*forkName`: A null-terminated string containing the name of the fork to look up (valid values: "main", "fsm", "vm", "init")
## Dependencies
- Functions called/Symbols referenced:
  - MAX_FORKNUM (constant defining maximum fork number)
  - forkNames (array containing fork name strings)
  - strcmp (standard C string comparison)
  - ereport/ERROR (PostgreSQL error reporting, backend only)
  - InvalidForkNumber (constant for invalid fork identifier)
- Called from (representative examples):
  - [pg_relation_size](../p/pg_relation_size.md) (in src/backend/utils/adt/dbsize.c:366)
  - [main](../m/main.md) (in src/bin/pg_waldump/pg_waldump.c:919)
  - FORKNAMECHARS (referenced in src/include/common/relpath.h:68)

## Notes and Other Information
- The function behavior differs between backend and frontend contexts: backend throws errors for invalid names while frontend returns InvalidForkNumber
- Valid fork names are "main", "fsm", "vm", and "init"
- Uses linear search which is acceptable given the small number of fork types
- Part of PostgreSQL's relation file path management system

## Simplified Source

```c
ForkNumber forkname_to_number(const char *forkName) {
    // Search through all valid fork names
    for (ForkNumber forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++) {
        if (strcmp(forkNames[forkNum], forkName) == 0) {
            return forkNum;
        }
    }

    // Handle not found case differently for backend vs frontend
#ifndef FRONTEND
    // Backend: throw error with helpful message
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("invalid fork name"),
             errhint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\".")));
#endif

    // Frontend: return invalid marker
    return InvalidForkNumber;
}
```