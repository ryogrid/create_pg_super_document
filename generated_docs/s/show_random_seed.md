# show_random_seed

## Location
[src/backend/commands/variable.c:669-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L669-L679)

## Overview
A function that returns the string representation of the current random seed setting for PostgreSQL's SHOW command, which is always "unavailable" as per the security design.

## Definition
```c
const char *show_random_seed(void)
```

## Detailed Description
The `show_random_seed` function is part of PostgreSQL's Grand Unified Configuration (GUC) system and serves as a show hook for the `random_seed` parameter. Unlike typical show functions that return the actual value of a configuration parameter, this function deliberately returns "unavailable" instead of the actual random seed value. This design choice is made for security reasons, as exposing the random seed could potentially compromise the cryptographic security of the system.

The function is called whenever a user executes `SHOW random_seed` or queries the `pg_settings` system view for the random_seed parameter. By returning "unavailable", PostgreSQL ensures that sensitive randomness information is not leaked to users who might exploit it.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (returns a string literal)
- Called from (representative examples):
  - GUC system when processing SHOW random_seed commands
  - pg_settings system view queries

## Notes and Other Information
- This function is part of a triplet of GUC hooks for random_seed: check_random_seed, assign_random_seed, and show_random_seed
- The "unavailable" return value is a deliberate security measure to prevent information leakage
- Located in src/backend/commands/variable.c alongside other GUC show functions
- Declared in src/include/utils/guc_hooks.h as part of the GUC hook interface