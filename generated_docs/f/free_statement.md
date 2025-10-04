# free_statement

## Location
[src/interfaces/ecpg/ecpglib/execute.c:96-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L96-L110)

## Overview
A static cleanup function that deallocates all memory associated with an ECPG statement structure and its components.

## Definition
```c
static void free_statement(struct statement *stmt)
```

## Detailed Description
The `free_statement` function performs comprehensive cleanup of an ECPG statement structure. It deallocates all associated memory including input and output variable lists, the SQL command string, statement name, and optionally the saved locale information (on systems without uselocale). The function handles NULL input gracefully and ensures complete cleanup to prevent memory leaks in ECPG applications.

## Parameters / Member Variables
- `stmt`: Pointer to the statement structure to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [statement](../s/statement.md) (struct type)
  - [free_variable](free_variable.md)
  - [ecpg_free](../e/ecpg_free.md)
- Called from (representative examples):
  - [ecpg_do_epilogue](../e/ecpg_do_epilogue.md)

## Notes and Other Information
- Safely handles NULL input by returning early
- Conditionally frees oldlocale member based on HAVE_USELOCALE macro availability
- Part of ECPG's resource management system for prepared statements
- Ensures complete cleanup of both input and output variable lists
- Critical for preventing memory leaks in applications using ECPG prepared statements

## Simplified Source

```c
static void
free_statement(struct statement *stmt)
{
    if (stmt == NULL)
        return;

    // Free input and output variable lists
    free_variable(stmt->inlist);
    free_variable(stmt->outlist);

    // Free statement strings
    ecpg_free(stmt->command);
    ecpg_free(stmt->name);

    // Free saved locale if needed (platform-dependent)
#ifndef HAVE_USELOCALE
    ecpg_free(stmt->oldlocale);
#endif

    // Free the statement structure itself
    ecpg_free(stmt);
}
```