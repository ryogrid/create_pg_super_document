# cleanup

## Location
[src/backend/bootstrap/bootstrap.c:677-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L677-L686)

## Overview
The cleanup function is a static utility function in the PostgreSQL bootstrap module that performs cleanup operations by closing any open relation descriptor if it exists.

## Definition

```c
static void
cleanup(void)
```
## Detailed Description
The cleanup function serves as a simple cleanup routine specifically designed for the bootstrap mode of PostgreSQL. It checks if there's an active relation descriptor (boot_reldesc) and if so, calls closerel() to properly close it. This function ensures that any relation opened during bootstrap operations is properly closed, preventing resource leaks and maintaining system consistency.

The function is part of the bootstrap subsystem which is responsible for initializing the PostgreSQL database system during the initial database creation process.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [closerel](closerel.md)
- Called from (representative examples):
  - [BootstrapModeMain](../B/BootstrapModeMain.md) (src/backend/bootstrap/bootstrap.c:367)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the bootstrap.c file
- The function relies on the global variable boot_reldesc to determine if cleanup is necessary
- It's specifically used during bootstrap mode operations to ensure proper resource cleanup
- The function is very simple but critical for preventing resource leaks during database initialization

## Simplified Source

```c
// Simplified version of cleanup
static void
cleanup(void)
{
    // Close any open relation descriptor during bootstrap
    if (boot_reldesc != NULL)
        closerel(NULL);
}
```

Key simplifications made:
- Added explanatory comment describing the purpose
- Function is already minimal, just added context comment
- Preserved essential logic: check for open relation and close it if needed
- Maintained the simple but critical resource cleanup pattern for bootstrap mode