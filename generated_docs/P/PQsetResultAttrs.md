# PQsetResultAttrs

## Location
[src/interfaces/libpq/fe-exec.c:249-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L249-L317)

## Overview
Sets the column attribute descriptors for a PGresult, including column names, types, and formats, with validation to prevent overwriting existing attributes.

## Definition

```c
int
PQsetResultAttrs(PGresult *res, int numAttributes, PGresAttDesc *attDescs)
```
## Detailed Description
PQsetResultAttrs configures the column metadata for a PGresult by copying the provided attribute descriptors. The function performs several validation checks: it ensures the result is valid (not NULL or OOM_result), prevents overwriting existing attributes, and handles no-op cases gracefully. When setting attributes, it allocates memory for the descriptors, performs a deep copy of the attribute names to ensure memory ownership, and determines the overall result format (binary vs text) based on individual column formats.

The function maintains the binary flag of the result - if any column uses text format (format == 0), the entire result is marked as non-binary. This ensures consistent format handling across all columns in the result set.

## Parameters / Member Variables
- `*res`: Target PGresult to set attributes for
- `numAttributes`: Number of columns in the result set
- `*attDescs`: Array of PGresAttDesc structures containing column metadata
## Dependencies
- Functions called/Symbols referenced:
  - [PQresultAlloc](PQresultAlloc.md)
  - [pqResultStrdup](../p/pqResultStrdup.md)
  - memcpy
- Called from (representative examples):
  - [PQcopyResult](PQcopyResult.md)

## Notes and Other Information
- Returns false (0) on failure, true (non-zero) on success
- Fails if attributes already exist in the result (cannot overwrite)
- Gracefully handles NULL or zero numAttributes as no-op
- Performs deep copy of attribute names to ensure proper memory management
- Automatically determines binary format based on individual column formats
- Uses result's memory context for all allocations to ensure proper cleanup

## Simplified Source

```c
int
PQsetResultAttrs(PGresult *res, int numAttributes, PGresAttDesc *attDescs)
{
    // Validate parameters
    if (!res || (const PGresult *) res == &OOM_result)
        return false;

    // Cannot overwrite existing attributes
    if (res->numAttributes > 0)
        return false;

    // Handle no-op request
    if (numAttributes <= 0 || !attDescs)
        return true;

    // Allocate memory for attribute descriptors
    res->attDescs = (PGresAttDesc *) PQresultAlloc(res, numAttributes * sizeof(PGresAttDesc));
    if (!res->attDescs)
        return false;

    res->numAttributes = numAttributes;
    memcpy(res->attDescs, attDescs, numAttributes * sizeof(PGresAttDesc));

    // Deep-copy attribute names and determine format
    res->binary = 1;  // Assume binary until we find text format
    for (int i = 0; i < res->numAttributes; i++)
    {
        if (res->attDescs[i].name)
            res->attDescs[i].name = pqResultStrdup(res, res->attDescs[i].name);
        else
            res->attDescs[i].name = res->null_field;

        if (!res->attDescs[i].name)
            return false;

        // If any column uses text format, mark entire result as non-binary
        if (res->attDescs[i].format == 0)
            res->binary = 0;
    }

    return true;
}
```