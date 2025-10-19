# SetVariableHooks

## Location
[src/bin/psql/variables.c:314-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L314-L366)

## Overview
Attaches substitute and/or assign hook functions to a named variable, creating the variable if it doesn't exist.

## Definition
```c
void SetVariableHooks(VariableSpace space, const char *name,
                      VariableSubstituteHook shook,
                      VariableAssignHook ahook)
```

## Detailed Description
The SetVariableHooks function is used to attach callback functions to variables in psql's variable system. It allows for custom processing whenever a variable's value is accessed or modified through substitute and assign hooks.

Key behaviors:
- If the variable exists, updates its hook functions and immediately executes them
- If the variable doesn't exist, creates it with a NULL value and attached hooks
- The substitute hook is called immediately after being set to potentially transform the current value
- The assign hook is called immediately after being set to validate/process the current value
- Either hook can be NULL if only one type of hook is needed
- Maintains the alphabetical ordering of variables in the linked list

The function is typically used during psql initialization to establish special variables that need custom behavior, such as variables that synchronize with internal psql state.

## Parameters / Member Variables
- `space`: VariableSpace (linked list head) to operate on
- `name`: Name of the variable to attach hooks to (must be valid variable name)
- `shook`: VariableSubstituteHook function pointer, or NULL if not needed
- `ahook`: VariableAssignHook function pointer, or NULL if not needed

## Dependencies
- Functions called/Symbols referenced:
  - [valid_variable_name](../v/valid_variable_name.md) (validates variable name format)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication function)
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation function)
  - strcmp (standard C string comparison)
- Data types referenced:
  - [VariableSpace](../V/VariableSpace.md)
  - struct _variable
  - VariableSubstituteHook (function pointer type)
  - VariableAssignHook (function pointer type)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (during psql initialization for setting up built-in variable hooks)

## Notes and Other Information
- Creates variables with NULL values if they don't exist, just to hold the hooks
- Immediately executes both hooks after setting them to initialize derived state
- Hook execution failures are ignored during setup - this is expected since no user value has been assigned yet
- Part of psql's variable system initialization - used extensively in EstablishVariableSpace
- Enables sophisticated variable behaviors like auto-sync with psql internal state
- No return value - function always succeeds or silently fails for invalid inputs

## Simplified Source

```c
void SetVariableHooks(VariableSpace space, const char *name,
                     VariableSubstituteHook shook,
                     VariableAssignHook ahook)
{
    struct _variable *current, *previous;

    // Basic validation
    if (!space || !name || !valid_variable_name(name))
        return;

    // Search for existing variable in sorted list
    for (previous = space, current = space->next; current;
         previous = current, current = current->next)
    {
        int cmp = strcmp(current->name, name);

        if (cmp == 0)  // Found existing variable
        {
            // Update hooks and execute them
            current->substitute_hook = shook;
            current->assign_hook = ahook;
            if (shook)
                current->value = (*shook)(current->value);
            if (ahook)
                (*ahook)(current->value);
            return;
        }
        if (cmp > 0)
            break;  // Variable doesn't exist
    }

    // Create new variable with hooks
    current = pg_malloc(sizeof(*current));
    current->name = pg_strdup(name);
    current->value = NULL;
    current->substitute_hook = shook;
    current->assign_hook = ahook;
    current->next = previous->next;
    previous->next = current;

    // Execute hooks on new variable
    if (shook)
        current->value = (*shook)(current->value);
    if (ahook)
        (*ahook)(current->value);
}
```