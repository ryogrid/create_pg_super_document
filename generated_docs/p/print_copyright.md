# print_copyright

## Location
[src/bin/psql/help.c:736-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/help.c#L736-L756)

## Overview
The print_copyright function displays the complete PostgreSQL copyright notice and license terms to standard output.

## Definition
void print_copyright(void)

## Detailed Description
This function prints the full PostgreSQL copyright and license information, including the PostgreSQL Database Management System identification, copyright notices for both the PostgreSQL Global Development Group and the University of California, and the complete BSD-style license terms. The output includes the permission statement, warranty disclaimers, and liability limitations that apply to PostgreSQL software distribution and use.

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - puts (standard C library function for output)
- Called from (representative examples):
  - [exec_command_copyright](../e/exec_command_copyright.md) (in src/bin/psql/command.c:740)

## Notes and Other Information
- This function implements the \\copyright backslash command in psql
- Displays the complete legal text for PostgreSQL distribution and usage rights
- The copyright notice includes both PostgreSQL Global Development Group (1996-2024) and University of California (1994) attributions
- Contains the full BSD-style license text with warranty disclaimers and liability limitations
- The function is simple and self-contained, using only the standard puts() function for output
- Located at src/bin/psql/help.c:736-756
- No return value - purely for display purposes

## Simplified Source

```c
void print_copyright(void) {
    // Print PostgreSQL copyright notice and BSD-style license
    puts("PostgreSQL Database Management System\n"
         "(also known as Postgres, formerly known as Postgres95)\n\n"
         "Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group\n\n"
         "Portions Copyright (c) 1994, The Regents of the University of California\n\n"
         // License terms...
         "Permission to use, copy, modify, and distribute this software...\n"
         // Full warranty disclaimers and liability limitations follow
         );
}
```