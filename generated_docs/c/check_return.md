# check_return

## Location
[src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.c:63-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.c#L63-L84)

## Overview
A static utility function that interprets and displays human-readable error messages for ECPG Informix compatibility error codes.

## Definition

```c
static void
check_return(int ret)
```
## Detailed Description
The check_return function serves as an error code interpreter for the ECPG (Embedded SQL in C for PostgreSQL) Informix compatibility layer. It takes an integer error code as input and outputs a corresponding human-readable error description. The function uses a switch statement to map specific ECPG Informix error constants to descriptive text, making it easier to understand test failures and debugging information.

This function is primarily used in test code to provide meaningful output when date/time formatting functions in the Informix compatibility layer encounter errors. It handles several specific error conditions related to date parsing and validation, and provides a fallback for unknown error codes.

## Parameters / Member Variables
- `ret`: An integer error code returned from ECPG Informix compatibility functions
## Dependencies
- Functions called/Symbols referenced:
  - printf
  - ECPG_INFORMIX_ENOTDMY
  - ECPG_INFORMIX_ENOSHORTDATE
  - ECPG_INFORMIX_BAD_DAY
  - ECPG_INFORMIX_BAD_MONTH
- Called from (representative examples):
  - [ECPGdebug](../E/ECPGdebug.md)
  - [date_test_strdate](../d/date_test_strdate.md)
  - [date_test_defmt](../d/date_test_defmt.md)
  - [date_test_fmt](../d/date_test_fmt.md)
  - [fmtlong](../f/fmtlong.md)

## Notes and Other Information
- This is a static function with internal linkage, accessible only within its compilation unit
- Handles specific Informix compatibility error codes:
  - ECPG_INFORMIX_ENOTDMY: Not a day-month-year format error
  - ECPG_INFORMIX_ENOSHORTDATE: No short date format error
  - ECPG_INFORMIX_BAD_DAY: Invalid day value error
  - ECPG_INFORMIX_BAD_MONTH: Invalid month value error
- Provides a default case for unknown error codes, displaying the raw error number
- Part of the ECPG test suite infrastructure for validating Informix compatibility features
- The function always prints a newline after the error description for clean output formatting
- Located in test expected output files, indicating its role in regression testing

## Simplified Source

```c
static void
check_return(int ret)
{
    // Map ECPG Informix error codes to human-readable messages
    switch(ret)
    {
        case ECPG_INFORMIX_ENOTDMY:
            printf("(ECPG_INFORMIX_ENOTDMY)");
            break;
        case ECPG_INFORMIX_ENOSHORTDATE:
            printf("(ECPG_INFORMIX_ENOSHORTDATE)");
            break;
        case ECPG_INFORMIX_BAD_DAY:
            printf("(ECPG_INFORMIX_BAD_DAY)");
            break;
        case ECPG_INFORMIX_BAD_MONTH:
            printf("(ECPG_INFORMIX_BAD_MONTH)");
            break;
        default:
            printf("(unknown ret: %d)", ret);
            break;
    }
    printf("\n");
}
```