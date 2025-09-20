# read_nondefault_variables

## Location
[src/backend/utils/misc/guc.c:5749-5821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5749-L5821)

## Overview
read_nondefault_variables deserializes non-default GUC configuration variables from a binary file to restore configuration state in newly spawned backend processes.

## Definition

```c
void
read_nondefault_variables(void)
```
## Detailed Description
read_nondefault_variables is the counterpart to write_nondefault_variables, responsible for reading and applying previously serialized GUC configuration variables. This function is called by newly spawned backend processes to inherit the configuration state from the postmaster.

The function operates in the following sequence:
1. **File Opening**: Opens CONFIG_EXEC_PARAMS file for reading (gracefully handles missing files)
2. **Deserialization Loop**: Reads variable records until EOF, each containing:
   - Variable name (null-terminated string)
   - Variable value (null-terminated string) 
   - Source file path (null-terminated string)
   - Source line number (binary integer)
   - Variable source (binary GucSource enum)
   - Variable source context (binary GucContext enum)
   - Variable source role OID (binary Oid)
3. **Validation**: Ensures each variable name exists in the configuration system
4. **Application**: Sets each variable using set_config_option_ext with original source information
5. **Source Attribution**: Records source file/line information when available
6. **Cleanup**: Frees allocated memory for strings

Error handling is strict - any format inconsistency results in FATAL errors since configuration corruption indicates serious system problems.

## Parameters / Member Variables
None - this function takes no parameters and operates on the global configuration state.

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile
  - FreeFile
  - [read_string_with_null](read_string_with_null.md)
  - find_option
  - set_config_option_ext
  - [set_config_sourcefile](../s/set_config_sourcefile.md)
  - [guc_free](../g/guc_free.md)
  - fread
  - ereport
  - elog
- Called from (representative examples):
  - [SubPostmasterMain](../S/SubPostmasterMain.md)

## Notes and Other Information
- Only available when EXEC_BACKEND is defined (Windows and some Unix configurations)
- Missing CONFIG_EXEC_PARAMS file is not an error (ENOENT is handled gracefully)
- Any other file access error or format inconsistency is FATAL
- Preserves original source attribution (file, line, context, role) for debugging
- Uses GUC_ACTION_SET with silent=true to avoid redundant processing
- Memory management uses PostgreSQL's GUC allocation functions (guc_free)
- Part of the EXEC_BACKEND mechanism that enables PostgreSQL on platforms without fork()
- The binary format read must exactly match what write_one_nondefault_variable produces
- [Variables](../V/Variables.md) are applied in the order they appear in the file
- Each variable is validated to exist before attempting to set its value