# PQgetlength

## Location
src/interfaces/libpq/fe-exec.c: 3887 - 3900

## Overview
PQgetlength returns the actual length in bytes of a field value in a PostgreSQL query result set.

## Definition
```c
int PQgetlength(const PGresult *res, int tup_num, int field_num)
```

## Detailed Description
PQgetlength retrieves the byte length of a specific field value in a query result. This is particularly useful for binary data or when you need to know the exact size of a field value, including cases where the data might contain embedded null bytes. The function performs bounds checking using check_tuple_field_number() before accessing the length information.

For NULL database values, the function returns 0. The length information is stored within the PGresult structure and represents the actual byte count of the data, not the length of a null-terminated string representation.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure containing the query results
- `tup_num`: Zero-based row number (tuple index) to retrieve data from
- `field_num`: Zero-based column number (field index) to retrieve data from

## Dependencies
- Functions called/Symbols referenced:
  - check_tuple_field_number
  - NULL_LEN
- Called from (representative examples):
  - libpqrcv_readtimelinehistoryfile
  - createViewAsClause
  - process_queued_fetch_requests
  - libpq_fetch_file
  - ecpg_get_data
  - ECPGget_desc
  - ecpg_store_result
  - do_field

## Notes and Other Information
- Returns 0 if the tuple or field number is out of range, or if the field value is NULL
- Essential for handling binary data where embedded null bytes are possible
- The length represents actual bytes stored, not string length
- NULL_LEN (-1) is used internally to mark NULL database values, but the function returns 0 for such cases
- Commonly used in conjunction with PQgetvalue() when precise data handling is required