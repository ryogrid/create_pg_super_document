# bytea_3

## Location
[src/interfaces/ecpg/test/expected/sql-bytea.c:60-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/sql-bytea.c#L60-L62)

## Overview
A struct definition used in PostgreSQL's ECPG test suite for handling variable-length bytea data through a pointer-based approach.

## Definition
```c
struct bytea_3 { int len; char arr[DATA_SIZE]; } *recv_vlen_buf;
```

## Detailed Description
The `bytea_3` struct is defined in the ECPG test suite and serves as a structure for handling variable-length bytea data through dynamic memory allocation. Unlike `bytea_1` and `bytea_2` which are instantiated as fixed arrays, this structure is declared as a pointer (`recv_vlen_buf`), allowing for dynamic allocation and variable-length data handling. The structure uses the DATA_SIZE constant for the array size but can be allocated dynamically as needed.

## Parameters / Member Variables
- `len`: Integer field that stores the length of the binary data contained in the array
- `arr`: Character array with size determined by DATA_SIZE constant that holds the actual binary data content

## Dependencies
- Functions called/Symbols referenced:
  - DATA_SIZE (constant used to define array size)
- Called from (representative examples):
  - init function (referenced at line 250)

## Notes and Other Information
- This structure is part of the ECPG test suite for validating bytea data type handling with variable-length data
- Uses pointer-based allocation (`recv_vlen_buf`) enabling dynamic memory management for variable-length scenarios
- The 'vlen' in the variable name suggests this is specifically designed for variable-length data testing
- More flexible than the fixed array approaches used in bytea_1 and bytea_2
- Supports testing scenarios where the data size may vary or needs to be allocated at runtime
- Used less frequently than the other bytea structures, indicating specialized use cases