# create_or_drop_command_generator

## Location
[src/bin/psql/tab-complete.c:5102-5130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5102-L5130)

## Overview
A common utility function used by PostgreSQL's psql tab completion system to generate command completions for CREATE and DROP statements while excluding specific entries based on provided flags.

## Definition


## Detailed Description
This is a core generator function in psql's tab completion system that provides autocompletion suggestions for SQL commands that can follow CREATE or DROP keywords. The function iterates through the  array to find matching command names that start with the given text prefix. It supports exclusion of certain commands based on flag bits, allowing different completion behaviors for CREATE vs DROP contexts.

The function follows the readline completion interface pattern, maintaining state between calls through static variables. On the first call (state == 0), it initializes the search parameters. On subsequent calls, it continues from where it left off, returning the next matching command until all possibilities are exhausted.

## Parameters / Member Variables
- : The partial command text that the user has typed so far
- : Call counter - 0 for first call, incremented on subsequent calls for the same completion
- : Bit flags indicating which command types should be excluded from completion results

## Dependencies
- Functions called/Symbols referenced:
  - : Case-insensitive string comparison
  - : Duplicates keyword with appropriate case handling
  - : Type definition for 32-bit flags
- Called from (representative examples):
  - 
  - 
  - 

## Notes and Other Information
This function is part of psql's sophisticated tab completion system that helps users write SQL commands more efficiently. It accesses the global  array which contains the master list of SQL commands that can follow CREATE/DROP keywords. The exclusion mechanism allows different generator functions to filter out inappropriate completions (e.g., DROP might exclude certain commands that are valid only for CREATE).