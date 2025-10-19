# PrintVariables

## Location
[src/bin/psql/variables.c:186-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L186-L210)

## Overview
Prints the values of all variables stored in a VariableSpace to standard output.

## Definition

```c
struct _variable *ptr;
```
## Detailed Description
The PrintVariables function iterates through all variables in the given VariableSpace and prints each variable's name and value in the format "name = 'value'" to standard output. The function safely handles NULL space parameters by returning immediately, and includes interrupt checking to allow cancellation during the printing process.

The function traverses a linked list of _variable structures starting from space->next, printing only variables that have non-NULL values. Each variable is displayed on a separate line with the format: variable_name = 'variable_value'.

## Parameters / Member Variables
- : A VariableSpace (pointer to _variable struct) representing the head of a linked list of variables to print

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
  - cancel_pressed (global variable for interrupt handling)
- Data types referenced:
  - [VariableSpace](../V/VariableSpace.md)
  - struct _variable
- Called from (representative examples):
  - [exec_command_set](../e/exec_command_set.md) (in src/bin/psql/command.c:2433)

## Notes and Other Information
- The function is part of psql's variable management system
- Safely handles NULL space parameters
- Includes interrupt checking via cancel_pressed to allow user cancellation
- Only prints variables that have non-NULL values
- Output format follows the pattern: variablename = 'variablevalue'

## Simplified Source

```c
void
PrintVariables(VariableSpace space)
{
    struct _variable *ptr;

    // Handle NULL space safely
    if (!space)
        return;

    // Traverse linked list and print each variable with a value
    for (ptr = space->next; ptr; ptr = ptr->next) {
        if (ptr->value)
            printf("%s = '%s'\n", ptr->name, ptr->value);

        // Allow user to cancel during printing
        if (cancel_pressed)
            break;
    }
}
```