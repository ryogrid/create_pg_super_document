# sqlda_compat_total_size

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:157-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L157-L170)

## Overview
Calculates the complete memory size required for a compatibility-mode SQLDA structure, including both metadata structures and data storage for a specific row.

## Definition

```c
static long
sqlda_compat_total_size(const PGresult *res, int row, enum COMPAT_MODE compat)
```
## Detailed Description
This function serves as a high-level wrapper that combines the calculation of empty SQLDA structure size with the data storage requirements for a specific row. It first calculates the base size needed for the SQLDA metadata (structure, field descriptors, field names) using sqlda_compat_empty_size, then adds the space required for storing the actual data values using sqlda_common_total_size. If a negative row index is provided, it returns only the empty structure size, allowing for metadata-only allocations.

This function provides the total memory footprint needed to allocate a complete compatibility-mode SQLDA that can hold both the structural information about the result set and the actual data values for the specified row.

## Parameters / Member Variables
- `*res`: Pointer to PostgreSQL result set containing data and metadata
- `row`: Zero-based row index to calculate storage for (negative values return empty size only)
- `compat`: Compatibility mode (ECPG_COMPAT or ECPG_INFORMIX) affecting type handling
## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_compat_empty_size](sqlda_compat_empty_size.md) (calculate empty structure size)
  - [sqlda_common_total_size](sqlda_common_total_size.md) (calculate data storage size)
  - COMPAT_MODE (enum type)
- Called from (representative examples):
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md)

## Notes and Other Information
This function is a key component in ECPG's compatibility-mode SQLDA implementation, providing memory allocation planning for complete SQLDA structures. The ability to handle negative row indices allows for flexible memory allocation scenarios where only metadata storage is needed initially. It's specifically designed for compatibility-mode SQLDAs, which maintain backward compatibility with older SQLDA interfaces. The function ensures that all memory requirements are properly calculated before allocation, preventing buffer overruns and ensuring adequate space for both structure and data.

## Simplified Source

```c
static long sqlda_compat_total_size(const PGresult *res, int row,
                                   enum COMPAT_MODE compat) {
    // Start with empty SQLDA structure size
    long offset = sqlda_compat_empty_size(res);

    // If no specific row, return just the structure size
    if (row < 0)
        return offset;

    // Add space for data values in the specified row
    offset = sqlda_common_total_size(res, row, compat, offset);
    return offset;
}
```