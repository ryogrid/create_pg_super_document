# StartupCommitTs

## Location
[src/backend/access/transam/commit_ts.c:632-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L632-L641)

## Overview
StartupCommitTs is a startup initialization function that activates the commit timestamp subsystem during postmaster or standalone backend startup.

## Definition
```c
void StartupCommitTs(void)
```

## Detailed Description
This function serves as a simple wrapper around ActivateCommitTs() and is specifically designed to be called exactly once during database system startup. It must be invoked after StartupXLOG has properly initialized the transaction system variables, particularly TransamVariables->nextXid, to ensure the commit timestamp system has access to valid transaction ID information.

The function is part of the startup sequence that prepares various PostgreSQL subsystems for normal operation.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
- Called from (representative examples):
  - [StartupXLOG](StartupXLOG.md)

## Notes and Other Information
- Must be called exactly ONCE during startup - calling it multiple times may cause undefined behavior
- Critical timing dependency: requires StartupXLOG to have completed initialization of TransamVariables->nextXid first
- Used in both postmaster and standalone backend startup scenarios
- The function essentially delegates all actual work to ActivateCommitTs, serving as a startup-specific entry point
- Declared in src/include/access/commit_ts.h for external visibility

## Simplified Source

```c
// Simplified version of StartupCommitTs
void StartupCommitTs(void) {
    // Activate the commit timestamp subsystem
    ActivateCommitTs();
}
```

Key simplifications made:
- Added explanatory comment
- This function is already very simple as it's just a wrapper
- Preserved the single function call which is the core functionality