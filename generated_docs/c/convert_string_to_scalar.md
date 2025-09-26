# convert_string_to_scalar

## Location
src/backend/utils/adt/selfuncs.c: 4527 - 4606

## Overview
Converts character-string data to a normalized scalar value between 0 and 1 for selectivity estimation purposes, optimizing the conversion by analyzing the byte value range and stripping common prefixes.

## Definition

```c
static void
convert_string_to_scalar(char *value,
						 double *scaledvalue,
						 char *lobound,
						 double *scaledlobound,
						 char *hibound,
						 double *scaledhibound)
```
## Detailed Description
This function performs the core work of  for character-string data types. It converts strings to a scale that ranges from 0 to 1 by treating the bytes of the string as fractional digits. The function employs several optimization strategies:

1. **Dynamic Range Analysis**: Instead of using a fixed base of 256, it analyzes the actual byte values present in the bounds to determine a more realistic range, which helps generate more accurate selectivity estimates.

2. **Character Class Expansion**: When the range includes certain character classes (uppercase ASCII, lowercase ASCII, or digits), it expands the range to include the full character class to account for typical data patterns.

3. **Minimum Range Enforcement**: If the computed range is too narrow (less than 10 characters), it defaults to the standard ASCII printable range (space to DEL) to ensure reasonable estimates.

4. **Common Prefix Stripping**: It removes any common prefix from all three input strings, allowing the algorithm to "zoom in" on the distinguishing portions of the strings. This is particularly effective for data like phone numbers where many values share a common area code.

## Parameters / Member Variables
- : The string value to be converted to a scalar
- : Output pointer for the scaled value of the input string
- : Lower bound string from histogram data
- : Output pointer for the scaled value of the lower bound
- : Upper bound string from histogram data  
- : Output pointer for the scaled value of the upper bound

## Dependencies
- Functions called/Symbols referenced:
  - convert_one_string_to_scalar (called 3 times for each string conversion)
- Called from (representative examples):
  - convert_to_scalar

## Notes and Other Information
- The function is static, indicating it's an internal implementation detail of the selfuncs.c module
- The algorithm specifically avoids using base 256 to prevent inflated selectivity estimates
- Character class expansion (A-Z, a-z, 0-9) reflects understanding of typical database content patterns
- Common prefix stripping is particularly valuable for hierarchical or formatted data like phone numbers, postal codes, or product codes
- The function is part of PostgreSQL's query planner's selectivity estimation system, crucial for generating optimal query plans