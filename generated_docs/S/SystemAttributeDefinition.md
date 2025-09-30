SystemAttributeDefinition

## Overview
SystemAttributeDefinition returns a Form_pg_attribute pointer for a system attribute given its negative attribute number, providing access to predefined system column metadata.

## Definition
const FormData_pg_attribute * SystemAttributeDefinition(AttrNumber attno)

## Detailed Description
SystemAttributeDefinition is a utility function that provides access to PostgreSQL system attributes (hidden columns) through their attribute numbers. System attributes have negative attribute numbers and include metadata columns like ctid, xmin, cmin, xmax, cmax, and tableoid that are present in every table but not visible in normal queries unless explicitly referenced.

The function validates the input attribute number and uses it as an index into the SysAtt array, which contains predefined FormData_pg_attribute structures for each system attribute. The function performs bounds checking and will error if an invalid system attribute number is provided.

## Parameters / Member Variables
- attno: The system attribute number (must be negative and within valid range). System attributes are numbered from -1 to -6 for ctid, xmin, cmin, xmax, cmax, and tableoid respectively.

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro to get array length)
  - elog (error logging function)
  - SysAtt (static array of system attribute definitions)
- Called from (representative examples):
  - [SPI_fname](SPI_fname.md) (in src/backend/executor/spi.c:1214)
  - [SPI_getvalue](SPI_getvalue.md) (in src/backend/executor/spi.c:1244)
  - [build_index_tlist](../b/build_index_tlist.md) (in src/backend/optimizer/util/plancat.c:1905)
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md) (in src/backend/parser/parse_relation.c:756)
  - [attnumAttName](../a/attnumAttName.md) (in src/backend/parser/parse_relation.c:3539)

## Notes and Other Information
- System attributes are defined as static FormData_pg_attribute structures (a1-a6) representing ctid, xmin, cmin, xmax, cmax, and tableoid
- The function uses negative indexing: attno -1 maps to SysAtt[0], -2 maps to SysAtt[1], etc.
- Input validation ensures the attribute number is negative and within the bounds of the SysAtt array
- These system attributes are fundamental to PostgreSQL MVCC (Multi-Version Concurrency Control) implementation
- Located in src/backend/catalog/heap.c:241-246

## Simplified Source

```c
const FormData_pg_attribute *
SystemAttributeDefinition(AttrNumber attno)
{
    // Validate system attribute number (must be negative and within bounds)
    if (attno >= 0 || attno < -(int) lengthof(SysAtt))
        elog(ERROR, "invalid system attribute number %d", attno);

    // Return system attribute definition from static array
    return SysAtt[-attno - 1];
}
```