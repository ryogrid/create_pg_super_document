# ecpg_build_params

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1213-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1213-L1580)

## Overview
Builds statement parameters by converting user variables into arrays compatible with PQexecParams(), handling various data types including descriptors, SQLDA structures, and regular variables.

## Definition

```c
bool
ecpg_build_params(struct statement *stmt)
```
## Detailed Description
This comprehensive function is the central parameter processing engine for ECPG statements. It processes the statement's input variable list and transforms them into parameter arrays that PostgreSQL's libpq can use. The function handles multiple parameter types including regular variables, SQL descriptors (ECPGt_descriptor), and SQLDA structures for compatibility with Informix. It performs client-side placeholder replacement for dynamic cursors and special /bin/bash placeholders, manages both text and binary parameter formats, and ensures proper memory allocation and error handling throughout the process.

## Parameters / Member Variables
- `*stmt`: Pointer to the statement structure containing the parameter list and command string to process
## Dependencies
- Functions called/Symbols referenced:
  - [PQparameterStatus](../P/PQparameterStatus.md)
  - [ecpg_find_desc](ecpg_find_desc.md)
  - [store_input_from_desc](../s/store_input_from_desc.md)
  - [ecpg_store_input](ecpg_store_input.md)
  - [next_insert](../n/next_insert.md)
  - [insert_tobeinserted](../i/insert_tobeinserted.md)
  - [convert_bytea_to_string](../c/convert_bytea_to_string.md)
  - [ecpg_alloc](ecpg_alloc.md)
  - [ecpg_realloc](ecpg_realloc.md)
  - [ecpg_free](ecpg_free.md)
  - [ecpg_free_params](ecpg_free_params.md)
  - [ecpg_raise](ecpg_raise.md)
- Called from:
  - [ecpg_do](ecpg_do.md)

## Notes and Other Information
- Returns true on successful parameter processing, false on error
- Handles three main variable types: descriptors, SQLDA structures, and regular variables
- Supports both Informix-compatible and standard SQLDA formats
- Manages client-side placeholder substitution for dynamic cursors (ECPGt_char_variable)
- Handles special /bin/bash placeholders that require client-side replacement
- Dynamically expands parameter arrays as needed using ecpg_realloc
- Converts old-style '?' placeholders to new-style '' format
- Performs comprehensive error checking for parameter count mismatches
- Critical component in the ECPG statement execution pipeline

## Simplified Source

```c
bool
ecpg_build_params(struct statement *stmt)
{
    struct variable *var;
    int desc_counter = 0;
    int position = 0;
    bool std_strings = false;

    // Check if standard_conforming_strings is enabled
    const char *value = PQparameterStatus(stmt->connection->connection, "standard_conforming_strings");
    if (value && strcmp(value, "on") == 0)
        std_strings = true;

    // Process each input variable
    var = stmt->inlist;
    while (var)
    {
        char *tobeinserted = NULL;
        int counter = 1;
        bool binary_format = false;
        int binary_length = 0;

        // Handle descriptor type - contains multiple variables
        if (var->type == ECPGt_descriptor)
        {
            struct descriptor *desc = ecpg_find_desc(stmt->lineno, var->pointer);
            if (!desc) return false;

            desc_counter++;
            for (struct descriptor_item *desc_item = desc->items; desc_item; desc_item = desc_item->next)
            {
                if (desc_item->num != desc_counter) continue;

                if (!store_input_from_desc(stmt, desc_item, &tobeinserted))
                    return false;

                if (desc_item->is_binary)
                {
                    binary_length = desc_item->data_len;
                    binary_format = true;
                }
                break;
            }
            if (desc->count == desc_counter)
                desc_counter = 0;
        }
        // Handle SQLDA type - similar to descriptor but different structure
        else if (var->type == ECPGt_sqlda)
        {
            // Process SQLDA structure (simplified - handles both Informix and standard modes)
            // Creates variable structure from SQLDA data and processes through ecpg_store_input
        }
        // Handle regular variables
        else
        {
            if (!ecpg_store_input(stmt->lineno, stmt->force_indicator, var, &tobeinserted, false))
                return false;

            if (var->type == ECPGt_bytea)
            {
                binary_length = ((struct ECPGgeneric_bytea *)(var->value))->len;
                binary_format = true;
            }
        }

        // Find position in command string for this parameter
        position = next_insert(stmt->command, position, stmt->questionmarks, std_strings) + 1;
        if (position == 0)
        {
            ecpg_raise(stmt->lineno, ECPG_TOO_MANY_ARGUMENTS,
                      ECPG_SQLSTATE_USING_CLAUSE_DOES_NOT_MATCH_PARAMETERS, NULL);
            ecpg_free_params(stmt, false);
            ecpg_free(tobeinserted);
            return false;
        }

        // Handle special cases for client-side replacement
        if (var->type == ECPGt_char_variable)
        {
            // Dynamic cursor - insert directly into command string
            int ph_len = (stmt->command[position] == '?') ? strlen("?") : strlen("$1");
            if (!insert_tobeinserted(position, ph_len, stmt, tobeinserted))
            {
                ecpg_free_params(stmt, false);
                return false;
            }
            tobeinserted = NULL;
        }
        else if (stmt->command[position] == '0')
        {
            // Special '$0' placeholder - client-side replacement
            if (stmt->statement_type == ECPGst_prepare || stmt->statement_type == ECPGst_exec_with_exprlist)
            {
                // Add quotes around statement name for PREPARE
                char *str = ecpg_alloc(strlen(tobeinserted) + 3, stmt->lineno);
                if (!str) return false;
                sprintf(str, "\"%s\"", tobeinserted);
                ecpg_free(tobeinserted);
                tobeinserted = str;
            }

            if (!insert_tobeinserted(position, 2, stmt, tobeinserted))
            {
                ecpg_free_params(stmt, false);
                return false;
            }
            tobeinserted = NULL;
        }
        else
        {
            // Regular parameter - add to parameter arrays
            // Reallocate parameter arrays to accommodate new parameter
            stmt->paramvalues = ecpg_realloc(stmt->paramvalues, sizeof(char *) * (stmt->nparams + 1), stmt->lineno);
            stmt->paramlengths = ecpg_realloc(stmt->paramlengths, sizeof(int) * (stmt->nparams + 1), stmt->lineno);
            stmt->paramformats = ecpg_realloc(stmt->paramformats, sizeof(int) * (stmt->nparams + 1), stmt->lineno);

            if (!stmt->paramvalues || !stmt->paramlengths || !stmt->paramformats)
            {
                ecpg_free_params(stmt, false);
                ecpg_free(tobeinserted);
                return false;
            }

            // Store parameter data
            stmt->paramvalues[stmt->nparams] = tobeinserted;
            stmt->paramlengths[stmt->nparams] = binary_length;
            stmt->paramformats[stmt->nparams] = (binary_format ? 1 : 0);
            stmt->nparams++;

            // Convert old-style '?' to new-style '$n' if needed
            if (stmt->command[position] == '?')
            {
                char *replacement = ecpg_alloc(20, stmt->lineno);
                if (!replacement) return false;
                snprintf(replacement, 20, "$%d", counter++);

                if (!insert_tobeinserted(position, 2, stmt, replacement))
                {
                    ecpg_free_params(stmt, false);
                    return false;
                }
            }
        }

        if (desc_counter == 0)
            var = var->next;
    }

    // Check for unmatched placeholders (except PREPARE statements)
    if (stmt->statement_type != ECPGst_prepare &&
        next_insert(stmt->command, position, stmt->questionmarks, std_strings) >= 0)
    {
        ecpg_raise(stmt->lineno, ECPG_TOO_FEW_ARGUMENTS,
                   ECPG_SQLSTATE_USING_CLAUSE_DOES_NOT_MATCH_PARAMETERS, NULL);
        ecpg_free_params(stmt, false);
        return false;
    }

    return true;
}
```