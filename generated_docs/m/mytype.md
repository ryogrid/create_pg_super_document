# mytype

## Location
src/interfaces/ecpg/test/expected/preproc-outofscope.c: 132 - 146

## Overview
mytype is a struct definition used in ECPG (Embedded SQL in C) test programs that represents a composite data record containing various data types commonly used in database operations.

## Definition
```c
struct mytype {
    int id;
    char t[64];
    double d1;
    double d2;
    char c[30];
};
```

## Detailed Description
The mytype struct serves as a fundamental data structure in PostgreSQL ECPG testing, designed to represent a typical database record with mixed data types. It contains fields for integer identification, variable-length character data, floating-point numeric values, and fixed-length character arrays. This structure is commonly used in embedded SQL operations to demonstrate how C programs can interface with PostgreSQL databases using structured data. The struct is typically used alongside its typedef alias MYTYPE and a corresponding null indicator structure mynulltype for complete database record handling.

## Parameters / Member Variables
- `id`: Integer field serving as a record identifier or primary key
- `t`: Character array of 64 bytes for storing variable-length text data
- `d1`: First double-precision floating-point field for numeric calculations
- `d2`: Second double-precision floating-point field for additional numeric data
- `c`: Character array of 30 bytes for storing shorter text strings

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls from struct definition)
- Called from (representative examples):
  - [MYTYPE](../M/MYTYPE.md) (line 112, used as typedef base)
  - [open_cur1](../o/open_cur1.md) (lines 212, 214, 216, 218, 220)
  - [get_record1](../g/get_record1.md) (lines 233, 235, 237, 239, 241)

## Notes and Other Information
- Located in src/interfaces/ecpg/test/expected/preproc-outofscope.c:132-146
- Forms the basis for the MYTYPE typedef alias
- Used extensively in ECPG test cases for validating embedded SQL functionality
- Demonstrates typical database record structure with mixed data types
- Part of the PostgreSQL testing framework for embedded C programs
- Works in conjunction with mynulltype struct for proper null value handling