# _becomeUser

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3416-3438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3416-L3438)

## Overview
A static function that changes the session authorization to a specified user while tracking state to avoid redundant SET SESSION AUTHORIZATION commands.

## Definition
```c
static void _becomeUser(ArchiveHandle *AH, const char *user)
```

## Detailed Description
This function serves as an optimized wrapper around session authorization changes during PostgreSQL restore operations. It maintains state tracking to prevent unnecessary SET SESSION AUTHORIZATION commands when the session user hasn't actually changed. The function handles NULL or empty user parameters by treating them as requests to restore the session default.

The optimization works by comparing the requested user with the currently tracked user (AH->currUser). If they match, no action is taken. If they differ, the function calls _doSetSessionAuth() to perform the actual authorization change and updates the internal tracking state.

The function maintains an "imaginary session user" concept that represents what user the restore script believes the session is running as, which is essential for generating correct restore scripts that can be replayed later.

## Parameters / Member Variables
- `AH`: Pointer to ArchiveHandle structure containing session state tracking
- `user`: Target username for session authorization, or NULL/empty string for session default

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison)
  - [_doSetSessionAuth](../d/_doSetSessionAuth.md) (actual authorization change function)
  - free (memory deallocation)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication utility)
- Called from (representative examples):
  - [_disableTriggersIfNecessary](../d/_disableTriggersIfNecessary.md)
  - [_enableTriggersIfNecessary](../e/_enableTriggersIfNecessary.md)
  - [_becomeOwner](_becomeOwner.md)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_backup_archiver.c:3416-3438
- Provides performance optimization by avoiding redundant authorization changes
- Essential for privilege management during restore operations where different objects may require different ownership contexts
- Maintains accurate state tracking for both connected and script generation modes
- Converts NULL user parameter to empty string to avoid null pointer issues
- Updates AH->currUser to track the current session user for future optimization checks

## Simplified Source

```c
static void _becomeUser(ArchiveHandle *AH, const char *user) {
    if (!user)
        user = "";  // Avoid null pointers

    // Skip if already the current user (optimization)
    if (AH->currUser && strcmp(AH->currUser, user) == 0)
        return;

    // Change session authorization
    _doSetSessionAuth(AH, user);

    // Update state tracking
    free(AH->currUser);
    AH->currUser = pg_strdup(user);
}
```