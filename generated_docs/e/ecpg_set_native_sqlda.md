# ecpg_set_native_sqlda

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:444-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L444-L592)

## Overview
Sets up and populates a native SQLDA structure with data values from a specific row of a PostgreSQL result set, handling proper data type conversion and memory layout for the native format.

## Definition

```c
void
ecpg_set_native_sqlda(int lineno, struct sqlda_struct **_sqlda, const PGresult *res, int row, enum COMPAT_MODE compat)
```
## Detailed Description
This function populates a pre-allocated native SQLDA structure with actual data values from a specified row in a PostgreSQL query result. Similar to ecpg_set_compat_sqlda, it handles the complex task of setting up data pointers within the SQLDA structure, performing proper memory alignment, and converting PostgreSQL result data into appropriate C data types based on the SQLDA field types.

The function processes each field in the native SQLDA, calculates proper memory offsets with alignment, sets up sqldata pointers to point to the correct locations within the SQLDA buffer, and converts PostgreSQL result data to the target data type. It handles the same data types as the compatibility version but uses the native sqlda_struct format instead of sqlda_compat. The main difference from the compatibility version is the absence of sqlilongdata handling for long strings and some differences in the structure layout.

## Parameters / Member Variables
- `lineno`: Line number for logging and debugging purposes
- `**_sqlda`: Double pointer to the sqlda_struct (native format) to be populated
- `*res`: PGresult structure containing the query results
- `row`: Row number in the result set to extract data from (negative values cause early return)
- `compat`: Compatibility mode that affects data type handling
## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_native_empty_size](../s/sqlda_native_empty_size.md)
  - [ecpg_sqlda_align_add_size](ecpg_sqlda_align_add_size.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - [ECPGset_noind_null](../E/ECPGset_noind_null.md)
  - [ecpg_get_data](ecpg_get_data.md)
  - [ecpg_log](ecpg_log.md)
- Called from (representative examples):
  - [ecpg_process_output](ecpg_process_output.md)

## Notes and Other Information
- This function works with the native SQLDA format (sqlda_struct) rather than the compatibility format (sqlda_compat)
- Does not allocate the SQLDA structure itself; only populates an existing one
- Handles the same wide variety of PostgreSQL data types as the compatibility version
- Special handling for numeric types includes copying digit buffers and adjusting internal pointers
- Unlike the compatibility version, does not handle sqlilongdata for very long strings
- Uses proper memory alignment to ensure efficient data access on different architectures
- Sets up NULL indicators for each field using predefined global values
- Part of the ECPG embedded SQL interface for PostgreSQL client applications providing native SQLDA support

## Simplified Source

```c
void ecpg_set_native_sqlda(int lineno, struct sqlda_struct **_sqlda,
                          const PGresult *res, int row, enum COMPAT_MODE compat) {
    struct sqlda_struct *sqlda = (*_sqlda);
    long offset, next_offset;

    if (row < 0)
        return;

    // Start offset after empty structure
    offset = sqlda_native_empty_size(res);

    // Set data pointers and convert values for each field
    for (int i = 0; i < sqlda->sqld; i++) {
        bool set_data = true;
        bool isnull = PQgetisnull(res, row, i);

        // Set up data pointer and length based on field type
        switch (sqlda->sqlvar[i].sqltype) {
            case ECPGt_short:
            case ECPGt_unsigned_short:
                ecpg_sqlda_align_add_size(offset, sizeof(short), sizeof(short),
                                        &offset, &next_offset);
                sqlda->sqlvar[i].sqldata = (char *) sqlda + offset;
                sqlda->sqlvar[i].sqllen = sizeof(short);
                break;

            case ECPGt_int:
            case ECPGt_unsigned_int:
                ecpg_sqlda_align_add_size(offset, sizeof(int), sizeof(int),
                                        &offset, &next_offset);
                sqlda->sqlvar[i].sqldata = (char *) sqlda + offset;
                sqlda->sqlvar[i].sqllen = sizeof(int);
                break;

            // ... similar handling for other numeric types ...

            case ECPGt_numeric: {
                // Special handling for variable-length numeric type
                set_data = false;
                ecpg_sqlda_align_add_size(offset, sizeof(NumericDigit *), sizeof(numeric),
                                        &offset, &next_offset);
                sqlda->sqlvar[i].sqldata = (char *) sqlda + offset;
                sqlda->sqlvar[i].sqllen = sizeof(numeric);

                if (!isnull) {
                    char *val = PQgetvalue(res, row, i);
                    numeric *num = PGTYPESnumeric_from_asc(val, NULL);
                    if (num) {
                        memcpy(sqlda->sqlvar[i].sqldata, num, sizeof(numeric));

                        // Copy digit buffer if present
                        if (num->buf) {
                            long digits_size = num->digits - num->buf + num->ndigits;
                            ecpg_sqlda_align_add_size(next_offset, sizeof(int), digits_size,
                                                    &offset, &next_offset);
                            memcpy((char *) sqlda + offset, num->buf, digits_size);

                            // Update pointers in the copied numeric struct
                            ((numeric *) sqlda->sqlvar[i].sqldata)->buf =
                                (NumericDigit *) sqlda + offset;
                            ((numeric *) sqlda->sqlvar[i].sqldata)->digits =
                                (NumericDigit *) sqlda + offset + (num->digits - num->buf);
                        }
                        PGTYPESnumeric_free(num);
                    }
                }
                break;
            }

            case ECPGt_string:
            default: {
                int datalen = strlen(PQgetvalue(res, row, i)) + 1;
                ecpg_sqlda_align_add_size(offset, sizeof(int), datalen,
                                        &offset, &next_offset);
                sqlda->sqlvar[i].sqldata = (char *) sqlda + offset;
                sqlda->sqlvar[i].sqllen = datalen;
                break;
            }
        }

        // Set up NULL indicator (native format doesn't have sqlilongdata)
        sqlda->sqlvar[i].sqlind = isnull ? &value_is_null : &value_is_not_null;

        // Convert and store the actual data value
        if (!isnull && set_data) {
            ecpg_get_data(res, row, i, lineno,
                         sqlda->sqlvar[i].sqltype, ECPGt_NO_INDICATOR,
                         sqlda->sqlvar[i].sqldata, NULL, 0, 0, 0,
                         ECPG_ARRAY_NONE, compat, false);
        }

        offset = next_offset;
    }
}
```