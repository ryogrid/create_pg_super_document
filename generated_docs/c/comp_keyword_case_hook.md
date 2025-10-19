# comp_keyword_case_hook

## Location
[src/bin/psql/startup.c:1048-1068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1048-L1068)

## Overview
A validation and assignment hook function for the COMP_KEYWORD_CASE psql variable that parses and validates user input to set the keyword case completion behavior.

## Definition
static bool comp_keyword_case_hook(const char *newval)

## Detailed Description
This function serves as a validation hook for the COMP_KEYWORD_CASE psql variable. It parses the provided string value and sets the corresponding completion case behavior in the global pset structure. The function validates that the input is one of the four supported case modes and returns false if an invalid value is provided. When successful, it updates pset.comp_case with the appropriate enum value that controls how SQL keywords are completed during tab completion.

## Parameters / Member Variables
- `newval`: The new string value being assigned to the COMP_KEYWORD_CASE variable. Must be one of: "preserve-upper", "preserve-lower", "upper", or "lower".

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (for case-insensitive string comparison)
  - [PsqlVarEnumError](../P/PsqlVarEnumError.md) (for error reporting on invalid values)
  - PSQL_COMP_CASE_PRESERVE_UPPER (enum constant)
  - PSQL_COMP_CASE_PRESERVE_LOWER (enum constant)
  - PSQL_COMP_CASE_UPPER (enum constant)
  - PSQL_COMP_CASE_LOWER (enum constant)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (via SetVariableHooks for COMP_KEYWORD_CASE variable)

## Notes and Other Information
- The function expects newval to never be NULL due to the substitute hook providing a default value
- Supports four completion modes: preserve user's case (upper/lower) or force to specific case (upper/lower)
- Uses case-insensitive comparison allowing flexibility in user input
- Returns false on validation failure, preventing the variable assignment
- Updates the global pset.comp_case field which is used by the tab completion system
- Located in src/bin/psql/startup.c:1048-1068

## Simplified Source

```c
static bool
comp_keyword_case_hook(const char *newval)
{
    Assert(newval != NULL);  // Substitute hook ensures non-NULL value

    // Set completion case mode based on string value (case-insensitive)
    if (pg_strcasecmp(newval, "preserve-upper") == 0)
        pset.comp_case = PSQL_COMP_CASE_PRESERVE_UPPER;
    else if (pg_strcasecmp(newval, "preserve-lower") == 0)
        pset.comp_case = PSQL_COMP_CASE_PRESERVE_LOWER;
    else if (pg_strcasecmp(newval, "upper") == 0)
        pset.comp_case = PSQL_COMP_CASE_UPPER;
    else if (pg_strcasecmp(newval, "lower") == 0)
        pset.comp_case = PSQL_COMP_CASE_LOWER;
    else
    {
        // Invalid value - show error and fail validation
        PsqlVarEnumError("COMP_KEYWORD_CASE", newval,
                         "lower, upper, preserve-lower, preserve-upper");
        return false;
    }

    return true;
}
```