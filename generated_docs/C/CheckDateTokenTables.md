# CheckDateTokenTables

## Location
[src/backend/utils/adt/datetime.c:4811-4839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4811-L4839)

## Overview
The main validation function that checks all PostgreSQL date/time token lookup tables and critical epoch constants during system startup.

## Definition
```c
bool CheckDateTokenTables(void)
```

## Detailed Description
This function serves as the primary entry point for validating PostgreSQL's date/time system integrity during postmaster startup. It performs comprehensive checks including validation of critical epoch date constants (Unix epoch and PostgreSQL epoch) and verification of the two main token lookup tables used for date/time parsing. The function ensures that all date/time subsystem components are properly configured before the database accepts connections, preventing potential runtime failures in date/time operations.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [CheckDateTokenTable](CheckDateTokenTable.md) (validates individual token tables)
  - [date2j](../d/date2j.md) (converts date to Julian day number)
  - UNIX_EPOCH_JDATE (Unix epoch constant validation)
  - POSTGRES_EPOCH_JDATE (PostgreSQL epoch constant validation)
  - datetktbl, szdatetktbl (main date/time token table and its size)
  - deltatktbl, szdeltatktbl (interval token table and its size)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (in src/backend/postmaster/postmaster.c)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckDateTokenTable](CheckDateTokenTable.md) (for validating individual token tables)
  - [date2j](../d/date2j.md) (for epoch date validation)
  - Assert (for critical constant validation)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (primary call during system startup)

## Notes and Other Information
- Called during postmaster startup to ensure date/time system integrity before accepting connections
- Uses assertions to validate critical epoch constants that are fundamental to PostgreSQL's date/time calculations
- Validates both the main date/time token table (datetktbl) and the interval token table (deltatktbl)
- Returns false if any validation fails, allowing the startup process to abort before potential runtime failures
- Part of PostgreSQL's defensive startup sequence that catches configuration or build problems early
- The epoch validations ensure that fundamental date calculations will work correctly throughout the system
- Critical for preventing subtle date/time bugs that could affect data integrity

## Simplified Source

```c
// Simplified version of CheckDateTokenTables
bool CheckDateTokenTables(void) {
    bool validation_passed = true;

    // Step 1: Validate critical epoch date constants
    // Ensure Unix epoch (1970-01-01) converts to expected Julian date
    Assert(UNIX_EPOCH_JDATE == date2j(1970, 1, 1));

    // Ensure PostgreSQL epoch (2000-01-01) converts to expected Julian date
    Assert(POSTGRES_EPOCH_JDATE == date2j(2000, 1, 1));

    // Step 2: Validate main date/time token lookup table
    validation_passed &= CheckDateTokenTable("datetktbl", datetktbl, szdatetktbl);

    // Step 3: Validate interval/delta token lookup table
    validation_passed &= CheckDateTokenTable("deltatktbl", deltatktbl, szdeltatktbl);

    // Return true only if all validations passed
    return validation_passed;
}
```

Key simplifications made:
- Added descriptive variable name `validation_passed` instead of cryptic `ok`
- Added step-by-step comments explaining each validation phase
- Clarified the purpose of each Assert statement with specific dates
- Made the boolean accumulation logic more explicit
- Focused on the main validation workflow without changing the core logic