# insert_tobeinserted

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1127-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1127-L1158)

## Overview
A static helper function that replaces a placeholder in an SQL command string with the actual parameter value or expression.

## Definition

```c
static bool
insert_tobeinserted(int position, int ph_len, struct statement *stmt, char *tobeinserted)
```
## Detailed Description
This function performs string manipulation to replace placeholders in SQL commands with actual values during ECPG parameter processing. It creates a new command string by concatenating three parts: the command up to the placeholder position, the replacement text, and the rest of the original command after the placeholder. The function handles memory allocation for the new string and ensures proper cleanup of both the old command and the replacement text.

## Parameters / Member Variables
- `position`: The 1-based position in the command string where replacement should occur
- `ph_len`: The length of the placeholder being replaced
- `*stmt`: Pointer to the statement structure containing the command to modify
- `*tobeinserted`: The replacement text to insert at the specified position
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - [ecpg_free](../e/ecpg_free.md)
- Called from:
  - [ecpg_build_params](../e/ecpg_build_params.md) (multiple locations)

## Notes and Other Information
- Returns true on successful replacement, false on memory allocation failure
- The function automatically frees the tobeinserted parameter regardless of success/failure
- Uses 1-based positioning for the replacement location
- Allocates a new command string and frees the old one, updating the statement structure
- Critical for parameter substitution in prepared SQL statements within ECPG
- Memory management includes cleanup of both input parameters and intermediate allocations

## Simplified Source

```c
static bool
insert_tobeinserted(int position, int ph_len, struct statement *stmt, char *tobeinserted)
{
    // Allocate new command string with space for replacement
    char *newcopy = ecpg_alloc(strlen(stmt->command) + strlen(tobeinserted) + 1, stmt->lineno);
    if (!newcopy) {
        ecpg_free(tobeinserted);
        return false;
    }

    // Build new command: [before placeholder] + [replacement] + [after placeholder]
    strcpy(newcopy, stmt->command);                    // Copy part before placeholder
    strcpy(newcopy + position - 1, tobeinserted);     // Insert replacement at position
    strcat(newcopy, stmt->command + position + ph_len - 1);  // Append part after placeholder

    // Replace old command with new one
    ecpg_free(stmt->command);
    stmt->command = newcopy;

    ecpg_free(tobeinserted);
    return true;
}
```