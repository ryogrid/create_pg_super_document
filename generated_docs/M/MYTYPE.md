# MYTYPE

## Location
[src/interfaces/ecpg/test/expected/preproc-outofscope.c:112-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-outofscope.c#L112-L124)

## Overview
MYTYPE is a typedef alias for the struct mytype, used in ECPG (Embedded SQL in C) test programs to provide a cleaner interface for handling structured data types in PostgreSQL embedded SQL operations.

## Definition

```c
typedef struct mytype  MYTYPE ;
```
## Detailed Description
MYTYPE serves as a convenient alias for struct mytype in the PostgreSQL ECPG testing framework. It is defined in the preproc-outofscope.c test file and represents a common pattern in C programming where typedef is used to create cleaner, more readable type names. This typedef allows functions to use MYTYPE instead of the more verbose "struct mytype" when declaring variables and function parameters. The underlying struct mytype contains multiple data fields commonly used in database operations including integer ID, character arrays for text data, and floating-point numbers for numeric calculations.

## Parameters / Member Variables
(MYTYPE references struct mytype members)
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Integer identifier field
- : Character array of 64 bytes for text storage  
- : First double-precision floating point field
- : Second double-precision floating point field
- : Character array of 30 bytes for additional text storage

## Dependencies
- Functions called/Symbols referenced:
  - [mytype](../m/mytype.md) (underlying struct)
- Called from (representative examples):
  - [get_var1](../g/get_var1.md) (lines 173, 180)
  - [open_cur1](../o/open_cur1.md) (lines 212, 214, 216, 218, 220)
  - [get_record1](../g/get_record1.md) (lines 233, 235, 237, 239, 241)
  - [main](../m/main.md) (lines 264, 330)

## Notes and Other Information
- Located in src/interfaces/ecpg/test/expected/preproc-outofscope.c:112
- Used extensively in ECPG test cases for validating embedded SQL functionality
- Part of the PostgreSQL testing framework for embedded C programs
- Provides type safety and code readability for database record structures