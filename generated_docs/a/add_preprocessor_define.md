# add_preprocessor_define

## Location
src/interfaces/ecpg/preproc/ecpg.c: 89 - 126

## Overview
Processes command-line -D switches to add preprocessor macro definitions to the ECPG preprocessor's global defines list.

## Definition
```c
static void add_preprocessor_define(char *define)
```

## Detailed Description
The `add_preprocessor_define` function handles command-line -D switches for the ECPG (Embedded SQL in C) preprocessor by parsing macro definitions and adding them to a global linked list of preprocessor defines. The function supports both simple macro definitions (which default to "1") and macro definitions with explicit values (NAME=VALUE format). It creates a new `_defines` structure for each macro and prepends it to the global `defines` list.

The function handles memory management carefully by creating copies of strings to avoid dependencies on argv storage, and it performs string parsing to separate macro names from their values while handling whitespace correctly.

## Parameters / Member Variables
- `define`: A string containing the macro definition from the command line, either in format "NAME" or "NAME=VALUE"

## Dependencies
- Functions called/Symbols referenced:
  - mm_strdup (ECPG string duplication function)
  - mm_alloc (ECPG memory allocation function)
  - strchr (standard C library function)
  - struct _defines (preprocessor definition structure)
  - defines (global variable maintaining the defines list head)

- Called from (representative examples):
  - main (in src/interfaces/ecpg/preproc/ecpg.c:208)

## Notes and Other Information
- The function is static and only accessible within the ecpg.c compilation unit
- Creates a complete copy of the input string to avoid relying on argv storage durability
- Supports two formats: simple definitions (NAME → value "1") and explicit definitions (NAME=VALUE)
- Strips whitespace between macro name and equals sign for cleaner parsing
- Uses prepending to the linked list for efficient insertion (new definitions added at head)
- Memory optimization: cmdvalue field points directly into the duplicated string rather than allocating separate memory
- The `used` field is initialized to NULL, likely for tracking macro usage during preprocessing
- No validation is performed on macro names or values - accepts any string input