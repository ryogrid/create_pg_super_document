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

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)
  - [read_string_with_null](read_string_with_null.md)
  - [find_option](../f/find_option.md)
  - [set_config_option_ext](../s/set_config_option_ext.md)
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

## Simplified Source

```c
void read_nondefault_variables(void)
{
    FILE *fp;
    char *varname, *varvalue, *varsourcefile;
    int varsourceline;
    GucSource varsource;
    GucContext varscontext;
    Oid varsrole;

    // Open configuration parameters file
    fp = AllocateFile(CONFIG_EXEC_PARAMS, "r");
    if (!fp)
    {
        // Missing file is OK, other errors are fatal
        if (errno != ENOENT)
            ereport(FATAL,
                    (errcode_for_file_access(),
                     errmsg("could not read from file \"%s\": %m",
                            CONFIG_EXEC_PARAMS)));
        return;
    }

    // Read and restore each variable
    for (;;)
    {
        // Read variable name (NULL indicates EOF)
        if ((varname = read_string_with_null(fp)) == NULL)
            break;

        // Validate variable exists
        if (find_option(varname, true, false, FATAL) == NULL)
            elog(FATAL, "failed to locate variable \"%s\" in exec config params file", varname);

        // Read variable metadata
        if ((varvalue = read_string_with_null(fp)) == NULL)
            elog(FATAL, "invalid format of exec config params file");
        if ((varsourcefile = read_string_with_null(fp)) == NULL)
            elog(FATAL, "invalid format of exec config params file");
        if (fread(&varsourceline, 1, sizeof(varsourceline), fp) != sizeof(varsourceline))
            elog(FATAL, "invalid format of exec config params file");
        if (fread(&varsource, 1, sizeof(varsource), fp) != sizeof(varsource))
            elog(FATAL, "invalid format of exec config params file");
        if (fread(&varscontext, 1, sizeof(varscontext), fp) != sizeof(varscontext))
            elog(FATAL, "invalid format of exec config params file");
        if (fread(&varsrole, 1, sizeof(varsrole), fp) != sizeof(varsrole))
            elog(FATAL, "invalid format of exec config params file");

        // Apply the variable setting with original source info
        (void) set_config_option_ext(varname, varvalue,
                                     varscontext, varsource, varsrole,
                                     GUC_ACTION_SET, true, 0, true);
        if (varsourcefile[0])
            set_config_sourcefile(varname, varsourcefile, varsourceline);

        // Free allocated strings
        guc_free(varname);
        guc_free(varvalue);
        guc_free(varsourcefile);
    }

    FreeFile(fp);
}
```