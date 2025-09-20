# sqlda_struct

## Location
[src/interfaces/ecpg/include/sqlda-native.h:33-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/include/sqlda-native.h#L33-L43)

## Overview
The  is the main SQL Descriptor Area structure in PostgreSQL's ECPG native interface, providing a complete descriptor for result sets or parameter lists with metadata and variable information.

## Definition

```c
struct sqlda_struct
{
	char		sqldaid[8];
	long		sqldabc;
	short		sqln;
	short		sqld;
	struct sqlda_struct *desc_next;
	struct sqlvar_struct sqlvar[1];
};
```
## Detailed Description
The  is the central data structure in PostgreSQL's ECPG native SQLDA (SQL Descriptor Area) implementation. It serves as a comprehensive descriptor that contains metadata about a complete set of SQL variables, whether they represent columns in a result set or parameters for a prepared statement. The structure follows the traditional SQLDA design pattern used in embedded SQL programming, providing both structural information (number of variables, total size) and actual variable descriptors.

This structure enables dynamic SQL operations where the schema or parameter structure is not known at compile time. It supports chaining of multiple SQLDA structures and contains an array of variable descriptors that describe individual columns or parameters. The design allows for efficient memory management and supports the full range of PostgreSQL data types through the embedded  elements.

## Parameters / Member Variables
- `sqldaid[8]`: An 8-character identifier string that typically contains "SQLDA" to identify the structure type
- `sqldabc`: A long integer representing the total byte count or size of the entire SQLDA structure
- `sqln`: A short integer indicating the maximum number of  entries that this SQLDA can accommodate
- `sqld`: A short integer representing the actual number of  entries currently in use
- `*desc_next`: A pointer to the next  in a chain, allowing for linked lists of SQLDA structures
- `sqlvar[1]`: A flexible array of  elements, each describing an individual SQL variable (actual size determined at runtime)

## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_struct](sqlda_struct.md) (self-reference for chaining)
  - [sqlvar_struct](sqlvar_struct.md)
- Called from (representative examples):
  - [ECPGdescribe](../E/ECPGdescribe.md)
  - ecpg_build_params
  - ecpg_process_output
  - [sqlda_native_empty_size](sqlda_native_empty_size.md)
  - [ecpg_set_compat_sqlda](../e/ecpg_set_compat_sqlda.md)
  - [ecpg_build_native_sqlda](../e/ecpg_build_native_sqlda.md)
  - [ecpg_set_native_sqlda](../e/ecpg_set_native_sqlda.md)

## Notes and Other Information
- This structure is part of PostgreSQL's ECPG native interface and is located in 
- The  array is a flexible array member - actual allocation includes space for the required number of  elements
- Used extensively throughout the ECPG library for dynamic SQL operations, parameter binding, and result set processing
- The chaining capability () allows for handling complex scenarios where multiple descriptor areas are needed
- Critical for applications that implement generic database access patterns or need to handle variable schemas
- The structure supports both input parameters (for prepared statements) and output variables (for result sets)
- Referenced in numerous test cases demonstrating ECPG functionality and SQLDA manipulation
- Follows the standard SQLDA format used in other embedded SQL implementations, ensuring compatibility and familiarity for developers