# sqlda_common_total_size

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:65-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L65-L156)

## Overview
Calculates the total memory size required to store all field values for a specific row in an SQLDA structure, accounting for proper data type alignment and storage requirements.

## Definition

```c
static long
sqlda_common_total_size(const PGresult *res, int row, enum COMPAT_MODE compat, long offset)
```
## Detailed Description
This function computes the memory space needed to store actual data values for all fields in a specific row of a PostgreSQL result set. It iterates through each field, determines the appropriate ECPG data type based on the PostgreSQL type and compatibility mode, then calculates the aligned storage requirements for each value. The function handles a comprehensive range of data types including numeric types, strings, dates, timestamps, decimals, and the complex numeric type which requires additional storage for digit arrays.

Special handling is provided for the numeric type, which requires deconstructing the value to determine the exact size needed for its variable-length digit array. The function ensures all values are properly aligned according to their type requirements for optimal memory access and compatibility across different architectures.

## Parameters / Member Variables
- `*res`: Pointer to PostgreSQL result set containing the data and metadata
- `row`: Zero-based row index in the result set to calculate storage for
- `compat`: Compatibility mode (ECPG_COMPAT or ECPG_INFORMIX) affecting type mapping
- `offset`: Starting memory offset where data storage calculations should begin
## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md) (get number of fields)
  - [sqlda_dynamic_type](sqlda_dynamic_type.md) (map PostgreSQL types to ECPG types)
  - [PQftype](../P/PQftype.md) (get field type)
  - [ecpg_sqlda_align_add_size](../e/ecpg_sqlda_align_add_size.md) (alignment calculations)
  - [PQgetisnull](../P/PQgetisnull.md) (check for NULL values)
  - [PQgetvalue](../P/PQgetvalue.md) (get field value as string)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md) (parse numeric values)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md) (free numeric resources)
  - Various ECPG type constants (ECPGt_short, ECPGt_int, etc.)
- Called from (representative examples):
  - [sqlda_compat_total_size](sqlda_compat_total_size.md)
  - [sqlda_native_total_size](sqlda_native_total_size.md)

## Notes and Other Information
This function is central to ECPG's SQLDA memory management, ensuring that sufficient space is allocated for data storage while maintaining proper alignment. The comprehensive type handling covers all standard SQL data types and their C equivalents. The numeric type handling is particularly complex due to its variable-length nature. The function is used by both compatibility and native SQLDA implementations, making it a shared utility for different SQLDA modes. Memory alignment is critical for performance and correctness, especially on architectures with strict alignment requirements.

## Simplified Source

```c
static long sqlda_common_total_size(const PGresult *res, int row,
                                   enum COMPAT_MODE compat, long offset) {
    int field_count = PQnfields(res);
    long next_offset;

    // Calculate storage size for each field value
    for (int i = 0; i < field_count; i++) {
        enum ECPGttype type = sqlda_dynamic_type(PQftype(res, i), compat);

        switch (type) {
            // Integer types
            case ECPGt_short:
            case ECPGt_unsigned_short:
                ecpg_sqlda_align_add_size(offset, sizeof(short), sizeof(short),
                                        &offset, &next_offset);
                break;

            case ECPGt_int:
            case ECPGt_unsigned_int:
                ecpg_sqlda_align_add_size(offset, sizeof(int), sizeof(int),
                                        &offset, &next_offset);
                break;

            case ECPGt_long:
            case ECPGt_unsigned_long:
                ecpg_sqlda_align_add_size(offset, sizeof(long), sizeof(long),
                                        &offset, &next_offset);
                break;

            case ECPGt_long_long:
            case ECPGt_unsigned_long_long:
                ecpg_sqlda_align_add_size(offset, sizeof(long long), sizeof(long long),
                                        &offset, &next_offset);
                break;

            // Floating point types
            case ECPGt_float:
                ecpg_sqlda_align_add_size(offset, sizeof(float), sizeof(float),
                                        &offset, &next_offset);
                break;

            case ECPGt_double:
                ecpg_sqlda_align_add_size(offset, sizeof(double), sizeof(double),
                                        &offset, &next_offset);
                break;

            // Special numeric type with variable-length digit array
            case ECPGt_numeric:
                ecpg_sqlda_align_add_size(offset, sizeof(NumericDigit *), sizeof(numeric),
                                        &offset, &next_offset);
                if (!PQgetisnull(res, row, i)) {
                    // Decode numeric to determine digit array size
                    char *val = PQgetvalue(res, row, i);
                    numeric *num = PGTYPESnumeric_from_asc(val, NULL);
                    if (num && num->buf) {
                        long digits_size = num->digits - num->buf + num->ndigits;
                        ecpg_sqlda_align_add_size(next_offset, sizeof(int), digits_size,
                                                &offset, &next_offset);
                    }
                    PGTYPESnumeric_free(num);
                }
                break;

            // Date/time types
            case ECPGt_date:
                ecpg_sqlda_align_add_size(offset, sizeof(date), sizeof(date),
                                        &offset, &next_offset);
                break;

            case ECPGt_timestamp:
                ecpg_sqlda_align_add_size(offset, sizeof(int64), sizeof(timestamp),
                                        &offset, &next_offset);
                break;

            // String types and default
            case ECPGt_char:
            case ECPGt_string:
            default: {
                long string_len = strlen(PQgetvalue(res, row, i)) + 1;
                ecpg_sqlda_align_add_size(offset, sizeof(int), string_len,
                                        &offset, &next_offset);
                break;
            }
        }
        offset = next_offset;
    }

    return offset;
}
```