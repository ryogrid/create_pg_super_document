# ecpg_store_result

## Location
[src/interfaces/ecpg/ecpglib/execute.c:303-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L303-L455)

## Overview
Stores query result data from a PostgreSQL result set into an ECPG variable structure, handling type conversion, memory allocation, and array processing for embedded SQL applications.

## Definition

```c
bool
ecpg_store_result(const PGresult *results, int act_field,
				  const struct statement *stmt, struct variable *var)
```
## Detailed Description
The  function is a core component of PostgreSQL's ECPG (Embedded SQL in C) library that transfers data from a PostgreSQL query result set into a client-side variable. This function handles complex scenarios including:

- Array type detection and validation
- Dynamic memory allocation for variable-sized data
- Type-specific data conversion and storage
- Indicator variable management for NULL value handling
- Special handling for character string arrays and varchar types
- Tuple count validation against array size constraints

The function performs comprehensive error checking and uses the ECPG error handling system to report issues such as cardinality violations, type mismatches, and memory allocation failures.

## Parameters / Member Variables
- `*results`: Pointer to the PostgreSQL result set containing the query data
- `act_field`: Zero-based index of the field/column in the result set to process
- `*stmt`: Pointer to the statement structure containing metadata like line number and compatibility mode
- `*var`: Pointer to the variable structure that will receive the data, including type information, size constraints, and storage pointers
## Dependencies
- Functions called/Symbols referenced:
  - : Get number of tuples in result set
  - : Determine if field type is an array
  - : Get field type from result set
  - : Raise ECPG errors
  - : Log ECPG messages
  - : Get field format (text/binary)
  - : Get field value as string
  - : Get field value length
  - : Allocate memory with error handling
  - : Convert and store individual field values
  - : Check compatibility mode

- Called from (representative examples):
  - : Descriptor-based data retrieval
  - : Main output processing function

## Notes and Other Information
- Supports both regular variables and arrays with comprehensive size validation
- Implements special handling for char** variables with dynamic memory layout
- Manages both data storage and optional indicator variables for NULL detection
- Uses ECPG-specific error codes and SQL state values for standardized error reporting
- Integrates with INFORMIX compatibility mode for legacy application support
- Memory allocation is handled through ECPG's auto-allocation system with automatic cleanup

## Simplified Source

```c
bool
ecpg_store_result(const PGresult *results, int act_field,
                  const struct statement *stmt, struct variable *var)
{
    enum ARRAY_TYPE isarray;
    int ntuples = PQntuples(results);
    bool status = true;

    // Check if field is an array type
    isarray = ecpg_is_type_an_array(PQftype(results, act_field), stmt, var);
    if (isarray == ECPG_ARRAY_ERROR) {
        ecpg_raise(stmt->lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    // Validate array size constraints
    if (isarray == ECPG_ARRAY_NONE) {
        // Check if we have enough space for all tuples
        if ((var->arrsize > 0 && ntuples > var->arrsize) ||
            (var->ind_arrsize > 0 && ntuples > var->ind_arrsize)) {
            ecpg_raise(stmt->lineno, ECPG_TOO_MANY_MATCHES, ECPG_SQLSTATE_CARDINALITY_VIOLATION, NULL);
            return false;
        }
    } else {
        // Array field requires array variable
        if (var->arrsize == 0) {
            ecpg_raise(stmt->lineno, ECPG_NO_ARRAY, ECPG_SQLSTATE_DATATYPE_MISMATCH, NULL);
            return false;
        }
    }

    // Allocate memory for variable-sized data if needed
    if ((var->arrsize == 0 || var->varcharsize == 0) && var->value == NULL) {
        int len = calculate_required_memory(results, act_field, var, ntuples);
        var->value = ecpg_auto_alloc(len, stmt->lineno);
        if (!var->value)
            return false;
        *((char **) var->pointer) = var->value;
    }

    // Allocate indicator variable if needed
    if (var->ind_value == NULL && var->ind_pointer != NULL) {
        int len = var->ind_offset * ntuples;
        var->ind_value = ecpg_auto_alloc(len, stmt->lineno);
        if (!var->ind_value)
            return false;
        *((char **) var->ind_pointer) = var->ind_value;
    }

    // Store data for each tuple
    if (is_char_pointer_array(var)) {
        // Special handling for char** arrays
        char **current_string = (char **) var->value;
        char *current_data_location = (char *) &current_string[ntuples + 1];

        for (int act_tuple = 0; act_tuple < ntuples && status; act_tuple++) {
            int len = strlen(PQgetvalue(results, act_tuple, act_field)) + 1;
            if (!ecpg_get_data(results, act_tuple, act_field, stmt->lineno,
                               var->type, var->ind_type, current_data_location,
                               var->ind_value, len, 0, var->ind_offset,
                               isarray, stmt->compat, stmt->force_indicator))
                status = false;
            else {
                *current_string = current_data_location;
                current_data_location += len;
                current_string++;
            }
        }
        *current_string = NULL;  // Terminate array
    } else {
        // Regular data storage
        for (int act_tuple = 0; act_tuple < ntuples && status; act_tuple++) {
            if (!ecpg_get_data(results, act_tuple, act_field, stmt->lineno,
                               var->type, var->ind_type, var->value,
                               var->ind_value, var->varcharsize, var->offset,
                               var->ind_offset, isarray, stmt->compat, stmt->force_indicator))
                status = false;
        }
    }

    return status;
}
```