# SPI_cursor_open_with_paramlist

## Location
[src/backend/executor/spi.c:1525-1532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1525-L1532)

## Overview
SPI_cursor_open_with_paramlist opens a prepared SPI plan as a portal (cursor) using PostgreSQL's internal ParamListInfo structure for parameter passing, providing more flexible parameter handling than traditional arrays.

## Definition
```c
Portal SPI_cursor_open_with_paramlist(const char *name, SPIPlanPtr plan, ParamListInfo params, bool read_only)
```

## Detailed Description
This function serves as a direct interface to SPI_cursor_open_internal, bypassing the parameter conversion step required by SPI_cursor_open. It accepts parameters in PostgreSQL's native ParamListInfo format, which provides several advantages:

1. **Dynamic Parameter Sets**: ParamListInfo supports dynamic parameter determination, allowing for more flexible parameter handling
2. **Efficient Parameter Passing**: Avoids the overhead of converting between different parameter representations
3. **Advanced Parameter Features**: Supports features like parameter hooks and custom parameter processing
4. **Direct Internal Access**: Provides the most direct path to the internal cursor opening mechanism

The function is essentially a thin wrapper around SPI_cursor_open_internal, making it the most efficient way to open cursors when you already have parameters in ParamListInfo format.

## Parameters / Member Variables
- `name`: Name to assign to the portal/cursor. Can be NULL for an unnamed portal.
- `plan`: Pointer to a previously prepared SPI plan (SPIPlanPtr) containing the compiled query.
- `params`: ParamListInfo structure containing parameter values and metadata. Can be NULL if the plan has no parameters.
- `read_only`: Boolean flag indicating whether the cursor should be read-only.

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md) (the core cursor opening implementation)
- Called from (representative examples):
  - Available through SPI interface for direct use
  - Used internally by other SPI functions that already have ParamListInfo structures

## Notes and Other Information
- This is the most efficient SPI cursor opening function when parameters are already in ParamListInfo format.
- [ParamListInfo](../P/ParamListInfo.md) provides advanced features like parameter hooks, dynamic parameter resolution, and custom parameter processing.
- The function performs no parameter validation or conversion, delegating all work to the internal implementation.
- Particularly useful for extensions and internal PostgreSQL code that work directly with ParamListInfo structures.
- Provides the foundation for more sophisticated parameter handling scenarios that the simpler array-based interfaces cannot support.
- The ParamListInfo structure can represent complex parameter scenarios including optional parameters and parameter-dependent queries.

## Simplified Source

```c
Portal
SPI_cursor_open_with_paramlist(const char *name, SPIPlanPtr plan,
                               ParamListInfo params, bool read_only)
{
    // Direct wrapper around internal cursor opening function
    // Provides efficient parameter passing using PostgreSQL's native ParamListInfo
    return SPI_cursor_open_internal(name, plan, params, read_only);
}
```