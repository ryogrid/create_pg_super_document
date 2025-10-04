# ECPGget_desc

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:234-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L234-L569)

## Overview
ECPGget_desc retrieves various descriptor items from a prepared SQL statement result, allowing applications to extract metadata and result data from dynamic SQL queries.

## Definition

```c
enum ECPGdtype type;
```
## Detailed Description
ECPGget_desc is a variadic function that extracts descriptor information from PostgreSQL result sets in ECPG applications. It processes a variable number of descriptor type/variable pairs, allowing retrieval of column metadata (name, type, length, precision, scale), result data, and indicators. The function supports both static and dynamic memory allocation for result data and handles proper locale settings for numeric data conversion.

The function operates by:
1. Validating parameters and retrieving the result set associated with the descriptor
2. Processing variable argument pairs of descriptor types and target variables
3. Extracting requested information based on the descriptor type (metadata, data, indicators)
4. Handling memory allocation for dynamic arrays when needed
5. Converting and storing data with proper locale handling for numeric types

## Parameters / Member Variables
- : Source code line number for error reporting and debugging
- : Name of the descriptor to retrieve information from
- : 1-based column index in the result set to process
- : Variable arguments consisting of ECPGdtype/variable pairs terminated by ECPGd_EODT

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - [ecpg_result_by_descriptor](../e/ecpg_result_by_descriptor.md)
  - [PQntuples](../P/PQntuples.md), PQnfields, PQfname, PQfsize, PQfmod, PQftype, PQgetlength, PQgetisnull
  - [get_char_item](../g/get_char_item.md), get_int_item
  - [ecpg_dynamic_type](../e/ecpg_dynamic_type.md), ecpg_dynamic_type_DDT
  - [ecpg_store_result](../e/ecpg_store_result.md)
  - [ecpg_auto_alloc](../e/ecpg_auto_alloc.md)
  - [ecpg_raise](../e/ecpg_raise.md), ecpg_log
- Called from (representative examples):
  - ECPG test programs (sql-describe.c, sql-dyntest.c, sql-dynalloc.c)
  - Dynamic SQL applications using DESCRIBE statements

## Notes and Other Information
- Supports multiple descriptor types: ECPGd_name, ECPGd_type, ECPGd_length, ECPGd_precision, ECPGd_scale, ECPGd_nullable, ECPGd_data, ECPGd_indicator, ECPGd_cardinality, ECPGd_ret_length, ECPGd_ret_octet
- Handles locale-specific numeric formatting by temporarily switching to C locale
- Provides automatic memory allocation for dynamic arrays when arrsize is 0
- Returns false on any error condition and sets appropriate SQLSTATE codes
- Thread-safe through proper locale handling mechanisms
- Critical for implementing SQL DESCRIBE functionality in ECPG applications

## Simplified Source

```c
bool ECPGget_desc(int lineno, const char *desc_name, int index, ...) {
    va_list args;
    PGresult *ECPGresult;
    enum ECPGdtype type;
    int ntuples, act_tuple;
    struct variable data_var;
    struct sqlca_t *sqlca = ECPGget_sqlca();

    // Basic validation
    if (sqlca == NULL) {
        ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    // Initialize and get result set
    va_start(args, index);
    ecpg_init_sqlca(sqlca);
    ECPGresult = ecpg_result_by_descriptor(lineno, desc_name);
    if (!ECPGresult) {
        va_end(args);
        return false;
    }

    ntuples = PQntuples(ECPGresult);

    // Validate column index
    if (index < 1 || index > PQnfields(ECPGresult)) {
        ecpg_raise(lineno, ECPG_INVALID_DESCRIPTOR_INDEX, ECPG_SQLSTATE_INVALID_DESCRIPTOR_INDEX, NULL);
        va_end(args);
        return false;
    }

    --index; // Convert to 0-based indexing

    // Initialize data variable structure
    memset(&data_var, 0, sizeof(data_var));
    data_var.type = ECPGt_EORT;
    data_var.ind_type = ECPGt_NO_INDICATOR;

    // Process variable arguments until ECPGd_EODT
    type = va_arg(args, enum ECPGdtype);
    while (type != ECPGd_EODT) {
        enum ECPGttype vartype = va_arg(args, enum ECPGttype);
        void *var = va_arg(args, void *);
        long varcharsize = va_arg(args, long);
        long arrsize = va_arg(args, long);
        long offset = va_arg(args, long);

        switch (type) {
            case ECPGd_data:
                // Set up data variable for result extraction
                data_var.type = vartype;
                data_var.pointer = var;
                data_var.varcharsize = varcharsize;
                data_var.arrsize = arrsize;
                data_var.offset = offset;
                break;

            case ECPGd_indicator:
                // Set up indicator variable
                data_var.ind_type = vartype;
                data_var.ind_pointer = var;
                data_var.ind_varcharsize = varcharsize;
                data_var.ind_arrsize = arrsize;
                data_var.ind_offset = offset;
                break;

            case ECPGd_name:
                // Get column name
                if (!get_char_item(lineno, var, vartype, PQfname(ECPGresult, index), varcharsize)) {
                    va_end(args);
                    return false;
                }
                break;

            case ECPGd_type:
                // Get column type
                if (!get_int_item(lineno, var, vartype, ecpg_dynamic_type(PQftype(ECPGresult, index)))) {
                    va_end(args);
                    return false;
                }
                break;

            case ECPGd_length:
                // Get column length
                if (!get_int_item(lineno, var, vartype, PQfmod(ECPGresult, index) - VARHDRSZ)) {
                    va_end(args);
                    return false;
                }
                break;

            case ECPGd_precision:
                // Get column precision
                if (!get_int_item(lineno, var, vartype, PQfmod(ECPGresult, index) >> 16)) {
                    va_end(args);
                    return false;
                }
                break;

            case ECPGd_scale:
                // Get column scale
                if (!get_int_item(lineno, var, vartype, (PQfmod(ECPGresult, index) - VARHDRSZ) & 0xffff)) {
                    va_end(args);
                    return false;
                }
                break;

            case ECPGd_cardinality:
                // Get number of tuples
                if (!get_int_item(lineno, var, vartype, PQntuples(ECPGresult))) {
                    va_end(args);
                    return false;
                }
                break;

            case ECPGd_ret_length:
            case ECPGd_ret_octet:
                // Get result lengths for all tuples
                if (arrsize > 0 && ntuples > arrsize) {
                    ecpg_raise(lineno, ECPG_TOO_MANY_MATCHES, ECPG_SQLSTATE_CARDINALITY_VIOLATION, NULL);
                    va_end(args);
                    return false;
                }

                // Auto-allocate memory if needed
                if (arrsize == 0 && *(void **)var == NULL) {
                    void *mem = ecpg_auto_alloc(offset * ntuples, lineno);
                    if (!mem) {
                        va_end(args);
                        return false;
                    }
                    *(void **)var = mem;
                    var = mem;
                }

                // Store length for each tuple
                for (act_tuple = 0; act_tuple < ntuples; act_tuple++) {
                    if (!get_int_item(lineno, var, vartype, PQgetlength(ECPGresult, act_tuple, index))) {
                        va_end(args);
                        return false;
                    }
                    var = (char *)var + offset;
                }
                break;

            default:
                // Unknown descriptor type
                ecpg_raise(lineno, ECPG_UNKNOWN_DESCRIPTOR_ITEM, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR, NULL);
                va_end(args);
                return false;
        }

        type = va_arg(args, enum ECPGdtype);
    }

    // Store actual result data if data variable was specified
    if (data_var.type != ECPGt_EORT) {
        struct statement stmt;
        memset(&stmt, 0, sizeof(stmt));
        stmt.lineno = lineno;

        // Set locale for numeric conversion
        stmt.connection = ecpg_get_connection(NULL);
        ecpg_store_result(ECPGresult, index, &stmt, &data_var);
    }

    // Handle indicator values
    if (data_var.ind_type != ECPGt_NO_INDICATOR && data_var.ind_pointer != NULL) {
        // Auto-allocate indicator memory if needed
        if (data_var.ind_arrsize == 0 && data_var.ind_value == NULL) {
            void *mem = ecpg_auto_alloc(data_var.ind_offset * ntuples, lineno);
            if (!mem) {
                va_end(args);
                return false;
            }
            *(void **)data_var.ind_pointer = mem;
            data_var.ind_value = mem;
        }

        // Store indicator values for all tuples
        for (act_tuple = 0; act_tuple < ntuples; act_tuple++) {
            if (!get_int_item(lineno, data_var.ind_value, data_var.ind_type,
                             -PQgetisnull(ECPGresult, act_tuple, index))) {
                va_end(args);
                return false;
            }
            data_var.ind_value = (char *)data_var.ind_value + data_var.ind_offset;
        }
    }

    sqlca->sqlerrd[2] = ntuples;
    va_end(args);
    return true;
}
```