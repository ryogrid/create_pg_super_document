# initcm

## Location
[src/backend/regex/regc_color.c:49-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L49-L102)

## Overview
Initializes a new colormap structure for regular expression compilation, setting up memory allocations and default color mappings.

## Definition

```c
static void
initcm(struct vars *v,
	   struct colormap *cm)
```
## Detailed Description
The  function initializes a colormap structure which is used during regular expression compilation to map characters to colors for efficient pattern matching. It sets up two types of color mappings:

1. **Low-range mapping** (): Maps characters from CHR_MIN to MAX_SIMPLE_CHR directly to colors
2. **High-range mapping** (): Uses a 2D array structure for characters above MAX_SIMPLE_CHR

The function allocates memory for these mappings and initializes the WHITE color descriptor, which represents the default color for most characters. It also sets up various counters and flags used by the colormap management system.

## Parameters / Member Variables
- `*v`: Pointer to vars structure containing compilation context and error handling
- `*cm`: Pointer to colormap structure to be initialized
## Dependencies
- Functions called/Symbols referenced:
  - MALLOC (memory allocation)
  - CERR (error reporting)
  - memset (memory initialization)
- Called from (representative examples):
  - CNOERR (in regcomp.c)

## Notes and Other Information
- The function relies on WHITE being zero for efficient memory initialization using memset
- Memory allocation failures are handled gracefully by setting error codes and preventing crashes during cleanup
- The initial allocation uses an arbitrary size of 4 rows for the high-range color array
- All characters initially map to the WHITE color until specific color assignments are made during regex compilation