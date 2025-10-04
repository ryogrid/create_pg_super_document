# get_int_item

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:108-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L108-L150)

## Overview
Converts and assigns an integer value to a variable of various numeric data types with appropriate type casting for ECPG descriptor operations.

## Definition

```c
static bool
get_int_item(int lineno, void *var, enum ECPGttype vartype, int value)
```
## Detailed Description
This internal utility function is part of ECPG's dynamic descriptor implementation that handles type conversion and assignment of integer values to variables of different numeric types. The function takes a generic void pointer to a variable, determines the target data type using the ECPG type enumeration, and performs appropriate casting and assignment.

The function supports a comprehensive range of integer, floating-point, and unsigned numeric types commonly used in C programming. It ensures type safety by performing explicit casts and validates that the target variable type is numeric, raising an appropriate error for non-numeric types.

## Parameters / Member Variables
- `lineno`: Source code line number for error reporting and debugging
- `*var`: Generic pointer to the target variable where the value will be stored
- `vartype`: ECPG type enumeration specifying the target variable's data type
- `value`: Integer value to be converted and assigned to the target variable
## Dependencies
- Functions called/Symbols referenced:
  - ECPGttype
  - ECPGt_short
  - ECPGt_int (implicitly through ECPGttype)
  - ECPGt_long
  - ECPGt_unsigned_short
  - ECPGt_unsigned_int
  - ECPGt_unsigned_long
  - ECPGt_long_long
  - ECPGt_unsigned_long_long
  - ECPGt_float
  - ECPGt_double
  - [ecpg_raise](../e/ecpg_raise.md)
  - ECPG_VAR_NOT_NUMERIC
  - ECPG_SQLSTATE_RESTRICTED_DATA_TYPE_ATTRIBUTE_VIOLATION
- Called from (representative examples):
  - [ECPGget_desc](../E/ECPGget_desc.md) (multiple calls for different descriptor field types)

## Notes and Other Information
- Static function, only accessible within descriptor.c
- Handles both signed and unsigned integer types, as well as floating-point types
- Returns true on successful assignment, false on type validation error
- Provides automatic type conversion with appropriate casting for numeric compatibility
- Essential component of ECPG's descriptor field value assignment mechanism
- Validates target type is numeric before attempting assignment

## Simplified Source

```c
static bool get_int_item(int lineno, void *var, enum ECPGttype vartype, int value) {
    // Convert and assign integer value based on target type
    switch (vartype) {
        case ECPGt_short:
            *(short *) var = (short) value;
            break;
        case ECPGt_int:
            *(int *) var = (int) value;
            break;
        case ECPGt_long:
            *(long *) var = (long) value;
            break;
        case ECPGt_unsigned_short:
            *(unsigned short *) var = (unsigned short) value;
            break;
        case ECPGt_unsigned_int:
            *(unsigned int *) var = (unsigned int) value;
            break;
        case ECPGt_unsigned_long:
            *(unsigned long *) var = (unsigned long) value;
            break;
        case ECPGt_long_long:
            *(long long int *) var = (long long int) value;
            break;
        case ECPGt_unsigned_long_long:
            *(unsigned long long int *) var = (unsigned long long int) value;
            break;
        case ECPGt_float:
            *(float *) var = (float) value;
            break;
        case ECPGt_double:
            *(double *) var = (double) value;
            break;
        default:
            // Error: target type is not numeric
            ecpg_raise(lineno, ECPG_VAR_NOT_NUMERIC,
                       ECPG_SQLSTATE_RESTRICTED_DATA_TYPE_ATTRIBUTE_VIOLATION, NULL);
            return false;
    }

    return true;
}
```