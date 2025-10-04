# PQgetlength

## Location
[src/interfaces/libpq/fe-exec.c:3887-3900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3887-L3900)

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
  - [check_tuple_field_number](../c/check_tuple_field_number.md)
  - NULL_LEN
- Called from (representative examples):
  - [libpqrcv_readtimelinehistoryfile](../l/libpqrcv_readtimelinehistoryfile.md)
  - [createViewAsClause](../c/createViewAsClause.md)
  - [process_queued_fetch_requests](../p/process_queued_fetch_requests.md)
  - [libpq_fetch_file](../l/libpq_fetch_file.md)
  - [ecpg_get_data](../e/ecpg_get_data.md)
  - [ECPGget_desc](../E/ECPGget_desc.md)
  - [ecpg_store_result](../e/ecpg_store_result.md)
  - [do_field](../d/do_field.md)

## Notes and Other Information
- Returns 0 if the tuple or field number is out of range, or if the field value is NULL
- Essential for handling binary data where embedded null bytes are possible
- The length represents actual bytes stored, not string length
- NULL_LEN (-1) is used internally to mark NULL database values, but the function returns 0 for such cases
- Commonly used in conjunction with PQgetvalue() when precise data handling is required

## Simplified Source

```c
int PQgetlength(const PGresult *res, int tup_num, int field_num)
{
    // Validate tuple and field numbers are in range
    if (!check_tuple_field_number(res, tup_num, field_num))
        return 0;

    // Return actual field length, or 0 for NULL values
    if (res->tuples[tup_num][field_num].len != NULL_LEN)
        return res->tuples[tup_num][field_num].len;
    else
        return 0;
}
```