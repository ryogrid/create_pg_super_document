# OPT_NULLS

## Location
[src/pl/tcl/pltcl.c:2698-2890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2698-L2890)

## Overview
OPT_NULLS is an enumeration constant used in the PL/Tcl extension to identify the `-nulls` command line option in the `pltcl_SPI_execute_plan` function.

## Definition
```c
enum options
{
    OPT_ARRAY, OPT_COUNT, OPT_NULLS
};
```

## Detailed Description
OPT_NULLS is part of an enumeration that defines the available command-line options for the `pltcl_SPI_execute_plan` function in the PL/Tcl procedural language extension. This constant specifically represents the `-nulls` option, which allows users to specify which parameters should be treated as NULL when executing a prepared SQL plan. The enumeration is used in conjunction with Tcl's `Tcl_GetIndexFromObj` function to parse command-line options and determine which specific option was provided by the user.

## Parameters / Member Variables
- This is an enumeration constant with no parameters or member variables

## Dependencies
- Functions called/Symbols referenced:
  - Used within pltcl_SPI_execute_plan function
  - Compared against optIndex variable in switch statement
- Called from (representative examples):
  - pltcl_SPI_execute_plan (case statement at line 2733)

## Notes and Other Information
- The OPT_NULLS option corresponds to the `-nulls` string in the options array
- When this option is selected, it expects a string argument specifying which parameters are NULL
- The nulls string length must match the number of query arguments
- Used in conjunction with `SPI_execute_plan` to handle NULL parameter values in prepared statements
- Part of the PL/Tcl extension's command-line option parsing mechanism