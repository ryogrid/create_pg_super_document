# CheckDateTokenTables

## Location
src/backend/utils/adt/datetime.c: 4811 - 4839

## Overview
The main validation function that checks all PostgreSQL date/time token lookup tables and critical epoch constants during system startup.

## Definition
```c
bool CheckDateTokenTables(void)
```

## Detailed Description
This function serves as the primary entry point for validating PostgreSQL's date/time system integrity during postmaster startup. It performs comprehensive checks including validation of critical epoch date constants (Unix epoch and PostgreSQL epoch) and verification of the two main token lookup tables used for date/time parsing. The function ensures that all date/time subsystem components are properly configured before the database accepts connections, preventing potential runtime failures in date/time operations.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CheckDateTokenTable (validates individual token tables)
  - date2j (converts date to Julian day number)
  - UNIX_EPOCH_JDATE (Unix epoch constant validation)
  - POSTGRES_EPOCH_JDATE (PostgreSQL epoch constant validation)
  - datetktbl, szdatetktbl (main date/time token table and its size)
  - deltatktbl, szdeltatktbl (interval token table and its size)
- Called from (representative examples):
  - PostmasterMain (in src/backend/postmaster/postmaster.c)

## Dependencies
- Functions called/Symbols referenced:
  - CheckDateTokenTable (for validating individual token tables)
  - date2j (for epoch date validation)
  - Assert (for critical constant validation)
- Called from (representative examples):
  - PostmasterMain (primary call during system startup)

## Notes and Other Information
- Called during postmaster startup to ensure date/time system integrity before accepting connections
- Uses assertions to validate critical epoch constants that are fundamental to PostgreSQL's date/time calculations
- Validates both the main date/time token table (datetktbl) and the interval token table (deltatktbl)
- Returns false if any validation fails, allowing the startup process to abort before potential runtime failures
- Part of PostgreSQL's defensive startup sequence that catches configuration or build problems early
- The epoch validations ensure that fundamental date calculations will work correctly throughout the system
- Critical for preventing subtle date/time bugs that could affect data integrity