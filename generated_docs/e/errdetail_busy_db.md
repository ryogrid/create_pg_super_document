# errdetail_busy_db

## Location
[src/backend/commands/dbcommands.c:3097-3126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3097-L3126)

## Overview
Generates detailed error messages explaining why a database operation failed due to active connections or prepared transactions.

## Definition

```c
static int
errdetail_busy_db(int notherbackends, int npreparedxacts)
```
## Detailed Description
This utility function creates user-friendly error detail messages when database operations (such as DROP DATABASE, CREATE DATABASE with template, RENAME DATABASE, or moving databases between tablespaces) cannot proceed because the target database is currently in use. The function intelligently formats messages based on the specific types of database activity preventing the operation.

The function handles three distinct scenarios: databases busy with both active connections and prepared transactions, databases with only active connections, or databases with only prepared transactions. It uses PostgreSQL's internationalization support with proper plural forms for better user experience across different languages.

The function returns 0 solely to satisfy the  macro requirements, as the actual purpose is the side effect of calling  or  to set the error detail message.

## Parameters / Member Variables
- `notherbackends`: Number of other backend processes (active connections) using the database
- `npreparedxacts`: Number of prepared transactions associated with the database
## Dependencies
- Functions called/Symbols referenced:
  -  - Set error detail message for cases with both backends and prepared transactions
  -  - Set error detail message with proper plural handling
- Called from:
  -  - When template database is busy during database creation
  -  - When target database cannot be dropped due to active usage
  -  - When database cannot be renamed due to active usage
  -  - When database cannot be moved between tablespaces due to active usage

## Notes and Other Information
- This is a static (internal) function, not exposed in the public API
- Uses PostgreSQL's gettext-based internationalization system for proper localization
- Implements intelligent message formatting based on the combination of active connections and prepared transactions
- The comment notes that gettext doesn't support multiple plurals in one string, explaining why different approaches are used for different scenarios
- Returns 0 as a placeholder to satisfy ereport macro requirements - the real work is done via side effects
- Part of PostgreSQL's comprehensive error reporting system to provide clear, actionable error messages to users
- The function is defined in
- Helps users understand exactly what is preventing their database operations from succeeding
- Critical for user experience as it transforms technical constraints into understandable explanations

## Simplified Source

```c
static int
errdetail_busy_db(int notherbackends, int npreparedxacts)
{
    // Generate appropriate error message based on what's blocking the database
    if (notherbackends > 0 && npreparedxacts > 0) {
        // Both active sessions and prepared transactions present
        errdetail("There are %d other session(s) and %d prepared transaction(s) using the database.",
                 notherbackends, npreparedxacts);
    }
    else if (notherbackends > 0) {
        // Only active sessions present
        errdetail_plural("There is %d other session using the database.",
                        "There are %d other sessions using the database.",
                        notherbackends, notherbackends);
    }
    else {
        // Only prepared transactions present
        errdetail_plural("There is %d prepared transaction using the database.",
                        "There are %d prepared transactions using the database.",
                        npreparedxacts, npreparedxacts);
    }

    return 0; // Required for ereport macro compatibility
}
```