# print_param_value

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1076-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1076-L1105)

## Overview
A static helper function that formats and logs parameter values for debugging purposes, handling both text and binary data formats.

## Definition

```c
static void
print_param_value(char *value, int len, int is_binary, int lineno, int nth)
```
## Detailed Description
This function is responsible for logging parameter values in a human-readable format as part of ECPG's parameter debugging functionality. It handles three different cases: null values, text values, and binary values. For binary data, it performs hexadecimal encoding to make the content readable in log output. The function ensures proper memory management by allocating temporary storage for hex-encoded values and freeing it after logging.

## Parameters / Member Variables
- : Pointer to the parameter value data (can be NULL)
- : Length of the parameter value in bytes
- : Flag indicating whether the value is binary (1) or text (0)
- : Source line number where the parameter logging occurs
- : Parameter number/index for identification in logs

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - [ecpg_hex_enc_len](../e/ecpg_hex_enc_len.md)
  - [ecpg_hex_encode](../e/ecpg_hex_encode.md)
  - [ecpg_log](../e/ecpg_log.md)
  - [ecpg_free](../e/ecpg_free.md)
- Called from:
  - [ecpg_free_params](../e/ecpg_free_params.md)

## Notes and Other Information
- This is a static function used internally within execute.c for debugging purposes
- For binary data, it allocates memory for hex encoding and ensures proper cleanup
- The function gracefully handles memory allocation failures by logging an error message
- All log output is directed through ecpg_log with a specific format identifying the parameter number and line number