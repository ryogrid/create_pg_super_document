# PQsetvalue

## Location
[src/interfaces/libpq/fe-exec.c:452-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L452-L542)

## Overview
Sets the value of a specific field in a PGresult tuple, with support for creating new tuples and proper NULL value handling.

## Definition

```c
int
PQsetvalue(PGresult *res, int tup_num, int field_num, char *value, int len)
```
## Detailed Description
PQsetvalue allows modification of field values within a PGresult, supporting both existing tuple modification and new tuple creation. When the specified tuple number equals the current tuple count, the function automatically creates a new tuple with all fields initialized to NULL. For existing tuples, it updates the specified field with the provided value.

The function performs comprehensive validation of parameters and handles various value types including NULL values (represented by NULL_LEN or NULL pointer), empty strings (len <= 0), and regular data values. Memory management is handled through the result's memory context, ensuring proper cleanup. All allocations include space for null termination of string values.

## Parameters / Member Variables
- `*res`: Target PGresult to modify
- `tup_num`: Tuple (row) number (0-based, can equal ntups to create new tuple)
- `field_num`: Field (column) number (0-based)
- `*value`: Pointer to the value data (can be NULL)
- `len`: Length of the value data (NULL_LEN for NULL values)
## Dependencies
- Functions called/Symbols referenced:
  - [check_field_number](../c/check_field_number.md)
  - [pqInternalNotice](../p/pqInternalNotice.md)
  - [pqResultAlloc](../p/pqResultAlloc.md)
  - [pqAddTuple](../p/pqAddTuple.md)
  - [libpq_gettext](../l/libpq_gettext.md)
  - memcpy
- Called from (representative examples):
  - [PQcopyResult](PQcopyResult.md)

## Notes and Other Information
- Returns true (non-zero) for success, false (0) for failure
- Automatically creates new tuples when tup_num equals current tuple count
- Handles NULL values through NULL_LEN constant or NULL value pointer
- All string values are null-terminated regardless of input length
- Reports errors through pqInternalNotice for proper error handling
- Uses result's memory context for all allocations to ensure cleanup
- Validates field numbers and tuple numbers with appropriate error messages
- Supports both text and binary data through the len parameter

## Simplified Source

```c
int
PQsetvalue(PGresult *res, int tup_num, int field_num, char *value, int len)
{
    PGresAttValue *attval;
    const char *errmsg = NULL;

    // Validate parameters
    if (!res || (const PGresult *) res == &OOM_result)
        return false;

    if (!check_field_number(res, field_num))
        return false;

    if (tup_num < 0 || tup_num > res->ntups)
    {
        pqInternalNotice(&res->noticeHooks,
                        "row number %d is out of range 0..%d",
                        tup_num, res->ntups);
        return false;
    }

    // Create new tuple if needed
    if (tup_num == res->ntups)
    {
        PGresAttValue *tup = (PGresAttValue *)
            pqResultAlloc(res, res->numAttributes * sizeof(PGresAttValue), true);

        if (!tup)
            goto fail;

        // Initialize all fields to NULL
        for (int i = 0; i < res->numAttributes; i++)
        {
            tup[i].len = NULL_LEN;
            tup[i].value = res->null_field;
        }

        // Add tuple to result
        if (!pqAddTuple(res, tup, &errmsg))
            goto fail;
    }

    attval = &res->tuples[tup_num][field_num];

    // Set field value
    if (len == NULL_LEN || value == NULL)
    {
        // NULL value
        attval->len = NULL_LEN;
        attval->value = res->null_field;
    }
    else if (len <= 0)
    {
        // Empty string
        attval->len = 0;
        attval->value = res->null_field;
    }
    else
    {
        // Copy value data
        attval->value = (char *) pqResultAlloc(res, len + 1, true);
        if (!attval->value)
            goto fail;

        attval->len = len;
        memcpy(attval->value, value, len);
        attval->value[len] = '\0';  // Null-terminate
    }

    return true;

fail:
    if (!errmsg)
        errmsg = libpq_gettext("out of memory");
    pqInternalNotice(&res->noticeHooks, "%s", errmsg);
    return false;
}
```