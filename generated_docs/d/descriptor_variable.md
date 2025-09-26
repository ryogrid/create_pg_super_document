# descriptor_variable

## Location
[src/interfaces/ecpg/preproc/descriptor.c:337-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L337-L350)

## Overview
Creates and returns a variable structure for SQL descriptors used in ECPG (Embedded SQL in C for PostgreSQL) preprocessing.

## Definition
```c
struct variable *descriptor_variable(const char *name, int input)
```

## Detailed Description
The `descriptor_variable` function creates a variable structure specifically for SQL descriptors in ECPG. It maintains two static variable instances to handle input and output descriptors separately. The function uses static storage to avoid repeated memory allocation and provides a simple interface for creating descriptor variables during SQL statement preprocessing.

The function operates by:
1. Using static arrays to store descriptor names and variable structures
2. Setting up a static ECPGtype structure with type ECPGt_descriptor
3. Copying the provided name into the appropriate static buffer
4. Returning a pointer to the corresponding static variable structure

## Parameters / Member Variables
- `name`: The name of the descriptor variable to create
- `input`: Integer flag indicating whether this is an input descriptor (0) or output descriptor (1)

## Dependencies
- Functions called/Symbols referenced:
  - [strlcpy](../s/strlcpy.md)
  - MAX_DESCRIPTOR_NAMELEN
  - ECPGt_descriptor
  - [ECPGtype](../E/ECPGtype.md)
- Called from (representative examples):
  - Used in ecpg.trailer for handling descriptor variables in SQL statements
  - Referenced in ECPG grammar processing

## Notes and Other Information
- Uses static storage for efficiency, maintaining only two variable instances
- The function is part of the ECPG preprocessor infrastructure
- Supports both input (0) and output (1) descriptors through the input parameter
- The returned variable structure is statically allocated and reused across calls
- Declared in preproc_extern.h for use throughout the ECPG preprocessor