# assign_client_encoding

## Location
[src/backend/commands/variable.c:756-798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L756-L798)

## Overview
A GUC assign hook function that actually applies a client encoding change after it has been validated by the check hook, with special handling for parallel worker processes.

## Definition
```c
void assign_client_encoding(const char *newval, void *extra)
```

## Detailed Description
The `assign_client_encoding` function is the assign hook for PostgreSQL's client_encoding configuration parameter. It is called after `check_client_encoding` has successfully validated the encoding change, and is responsible for actually implementing the encoding change in the current process.

The function handles several important scenarios:
1. **Parallel Worker Handling**: Special logic for parallel worker processes, which send data to the leader process rather than directly to clients
2. **Initialization vs Runtime Changes**: During parallel worker initialization, encoding changes are accepted to maintain consistency with the leader process
3. **Runtime Restrictions**: Non-initialization changes in parallel workers are rejected since they cannot effectively communicate encoding changes to the leader
4. **Encoding Application**: Calls `SetClientEncoding` to actually implement the encoding change in the current process
5. **Error Handling**: Logs any unexpected failures from `SetClientEncoding`, though such failures should not occur if the check hook succeeded

## Parameters / Member Variables
- `newval`: The canonical name of the new client encoding (not actually used, since the encoding ID is passed via extra)
- `extra`: Pointer to the encoding ID integer that was stored by `check_client_encoding`

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [SetClientEncoding](../S/SetClientEncoding.md)
  - ereport (via ERROR)
  - elog
  - InitializingParallelWorker (global variable)
- Called from (representative examples):
  - GUC system after successful validation by check_client_encoding
  - SET CLIENT_ENCODING command execution
  - Configuration parameter changes during server startup

## Notes and Other Information
- This function is part of a triplet of GUC hooks for client_encoding: check_client_encoding, assign_client_encoding, and show_client_encoding
- The encoding ID is retrieved from the extra parameter that was set by check_client_encoding
- Parallel worker processes have special restrictions because they communicate through the leader process, not directly with clients
- The newval parameter is not used since the actual encoding ID is passed via the extra parameter
- Any failure in SetClientEncoding is logged but not treated as fatal, since the check hook should have prevented invalid assignments
- Located in src/backend/commands/variable.c alongside other encoding-related functions
- The function assumes that if check_client_encoding succeeded, SetClientEncoding should also succeed