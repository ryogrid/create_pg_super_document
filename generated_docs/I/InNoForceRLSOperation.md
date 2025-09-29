# InNoForceRLSOperation

## Location
[src/backend/utils/init/miscinit.c:694-706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L694-L706)

## Overview
Checks whether PostgreSQL should ignore FORCE ROW LEVEL SECURITY directives in the current execution context by examining security restriction flags.

## Definition
```c
bool InNoForceRLSOperation(void)
```

## Detailed Description
InNoForceRLSOperation is a security query function that determines if the current execution context should bypass or ignore Row Level Security (RLS) FORCE directives. It examines the SECURITY_NOFORCE_RLS bit in the SecurityRestrictionContext global variable.

This function is part of PostgreSQL's Row Level Security (RLS) system, which provides fine-grained access control at the row level. When this flag is set, it indicates that certain operations should not be subject to forced RLS policies, typically in contexts where bypassing RLS is necessary for system functionality or security-definer operations.

## Parameters / Member Variables
None - this function takes no parameters and returns a boolean value.

## Dependencies
- Functions called/Symbols referenced:
  - SecurityRestrictionContext (global variable)
  - SECURITY_NOFORCE_RLS (macro constant: 0x0004)
- Called from (representative examples):
  - [check_enable_rls](../c/check_enable_rls.md)
  - AmSpecialWorkerProcess

## Notes and Other Information
- Returns true when SECURITY_NOFORCE_RLS flag is set in SecurityRestrictionContext
- Primarily used by PostgreSQL's Row Level Security (RLS) enforcement system
- Allows certain privileged operations to bypass forced RLS policies when necessary
- Part of the broader security context management system in PostgreSQL
- Critical for maintaining proper RLS behavior in security-elevated contexts
- Used in conjunction with other security restriction flags to provide comprehensive access control

## Simplified Source
```c
/*
 * InNoForceRLSOperation - are we ignoring FORCE ROW LEVEL SECURITY ?
 */
bool
InNoForceRLSOperation(void)
{
    return (SecurityRestrictionContext & SECURITY_NOFORCE_RLS) != 0;
}
```