# set_var_from_var

## Location
src/backend/utils/adt/numeric.c: 7484 - 7509

## Overview
Copies one NumericVar variable into another, allocating new memory for the destination and including a spare digit for rounding operations.

## Definition


## Detailed Description
This static function performs a deep copy of a NumericVar variable, creating an independent copy with its own allocated memory buffer. The function allocates a new digit buffer that is one digit larger than the source to provide a spare digit for rounding operations. The spare digit is placed at the beginning of the buffer and initialized to zero, with the actual digits copied to positions starting from index 1.

The function first handles memory management by freeing any existing buffer in the destination variable, then performs a complete structure copy using memmove(), and finally updates the buffer and digits pointers to point to the newly allocated memory. This ensures the destination is completely independent of the source.

## Parameters / Member Variables
- `value`: Source NumericVar to copy from (const pointer indicating read-only access)
- `dest`: Destination NumericVar that will receive the copied data

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_alloc (allocates memory for digit buffer)
  - digitbuf_free (frees existing digit buffer)
  - NumericDigit (typedef for individual digit storage)
  - memcpy (copies digit array)
  - memmove (copies entire structure)
- Called from (representative examples):
  - generate_series_step_numeric (numeric series generation)
  - width_bucket_numeric (histogram bucket calculations)
  - [compute_bucket](../c/compute_bucket.md) (bucket computation utilities)
  - [div_mod_var](../d/div_mod_var.md) (division and modulo operations)
  - [ceil_var](../c/ceil_var.md) (ceiling function implementation)
  - [floor_var](../f/floor_var.md) (floor function implementation)
  - [sqrt_var](sqrt_var.md) (square root calculations)
  - [power_var](../p/power_var.md) (exponentiation operations)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c module
- Allocates buffer with one extra digit for rounding operations
- The spare digit is always positioned at index 0 and initialized to zero
- Properly handles memory management by freeing existing destination buffer
- Handles the case where source digits might be NULL (when ndigits is 0)
- The destination becomes completely independent of the source after copying
- Used extensively in numeric operations that need to preserve intermediate results
- Essential for operations that require temporary copies or backup values