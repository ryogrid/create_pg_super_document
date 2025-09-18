# checkInitSteps

## Location
src/bin/pgbench/pgbench.c: 5239 - 5258

## Overview
Validates an initialization-steps string to ensure all specified characters correspond to valid pgbench initialization steps.

## Definition
```c
static void checkInitSteps(const char *initialize_steps)
```

## Detailed Description
This function performs validation of the initialization steps string provided via the -I command-line option to pgbench. It serves as an early validation mechanism to catch invalid step characters during option parsing rather than waiting until the actual initialization process begins, providing a better user experience.

The function checks two conditions:
1. The string is not empty (at least one step must be specified)
2. Every character in the string is either a valid initialization step character (as defined in ALL_INIT_STEPS: "dtgGvpf") or a space (spaces are allowed as separators)

If validation fails, the function logs an error message indicating the invalid character and lists all allowed step characters, then exits with status 1.

## Parameters / Member Variables
- `initialize_steps`: String containing initialization step characters to validate (typically from -I command line option)

## Dependencies
- Functions called/Symbols referenced:
  - ALL_INIT_STEPS (constant defining valid step characters: "dtgGvpf")
  - strchr (standard C library function to search for character)
  - pg_fatal (PostgreSQL utility for fatal error logging)
  - pg_log_error (PostgreSQL utility for error logging)
  - pg_log_error_detail (PostgreSQL utility for detailed error logging)
  - exit (standard C library exit function)
- Called from (representative examples):
  - main (during command-line option processing)

## Notes and Other Information
- Valid initialization step characters are defined by ALL_INIT_STEPS: "dtgGvpf" where:
  - d = drop existing tables
  - t = create tables  
  - g = generate data
  - G = generate data with vacuum
  - v = create views
  - p = create primary keys
  - f = create foreign keys
- Spaces are allowed in the initialization string as separators
- This function exits the program on validation failure rather than returning an error code
- Provides user-friendly error messages that show both the invalid character and the list of valid characters
- Located in src/bin/pgbench/pgbench.c:5239-5258