# deccopy

## Location
src/interfaces/ecpg/compatlib/informix.c: 173 - 178

## Overview
Copies the contents of one decimal variable to another, providing a simple and efficient way to duplicate decimal values in Informix compatibility mode.

## Definition
```c
void deccopy(decimal *src, decimal *target)
```

## Detailed Description
The `deccopy` function implements decimal value copying for Informix compatibility in ECPG. It performs a straightforward memory copy operation using `memcpy` to transfer the entire decimal structure from source to target. This function provides the standard Informix decimal copy semantics, ensuring that all internal decimal representation data is properly duplicated.

## Parameters / Member Variables
- `src`: Pointer to the source decimal variable to copy from
- `target`: Pointer to the target decimal variable to copy to

## Dependencies
- Functions called/Symbols referenced:
  - memcpy
  - decimal (type)
- Called from (representative examples):
  - ECPG applications using Informix decimal compatibility

## Notes and Other Information
- Part of the public Informix decimal compatibility API
- Performs a complete bitwise copy of the decimal structure
- No return value - assumes valid pointers are provided
- Does not perform any validation or null checking
- Essential for decimal variable assignment and value preservation
- Simple but critical function for Informix decimal compatibility layer