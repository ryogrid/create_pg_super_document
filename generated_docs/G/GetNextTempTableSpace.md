# GetNextTempTableSpace

## Location
[src/backend/storage/file/fd.c:3108-3128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3108-L3128)

## Overview
Selects the next temporary tablespace to use in round-robin fashion, advancing the internal counter with wraparound for even distribution of temporary files.

## Definition
```c
Oid GetNextTempTableSpace(void)
```

## Detailed Description
The `GetNextTempTableSpace` function implements a round-robin selection mechanism for choosing temporary tablespaces. Each call to this function advances an internal counter (`nextTempTableSpace`) and returns the tablespace OID at that position in the configured temporary tablespace array.

The function includes wraparound logic to ensure that when the counter reaches the end of the tablespace array, it resets to zero and continues from the beginning. This circular behavior ensures that temporary files are distributed evenly across all configured tablespaces over time, which helps balance I/O load.

If no temporary tablespaces are configured (`numTempTableSpaces` is 0 or negative), the function returns `InvalidOid`, indicating that the current database's default tablespace should be used.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (direct access to global variables)

- Global variables accessed:
  - `numTempTableSpaces` - Count of configured temporary tablespaces
  - `nextTempTableSpace` - Index counter for round-robin selection (modified)
  - `tempTableSpaces` - Array of configured temporary tablespace OIDs
  - `InvalidOid` - Constant representing an invalid/default OID

- Called from (representative examples):
  - `[GetDefaultTablespace](GetDefaultTablespace.md)` (src/backend/commands/tablespace.c:1151)
  - `[OpenTemporaryFile](../O/OpenTemporaryFile.md)` (src/backend/storage/file/fd.c:1745)

## Notes and Other Information
- Implements round-robin distribution to ensure even spread of temporary files across tablespaces
- The internal counter is advanced on each call, providing stateful behavior across multiple invocations
- Wraparound logic ensures continuous cycling through all available tablespaces
- Returns InvalidOid when no temporary tablespaces are configured, indicating fallback to default tablespace
- Critical for load balancing in systems with multiple temporary tablespaces
- The returned OID may itself be InvalidOid if that was stored in the tablespace array, indicating default tablespace usage for that position