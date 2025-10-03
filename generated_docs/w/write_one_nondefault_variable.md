# write_one_nondefault_variable

## Location
[src/backend/utils/misc/guc.c:5594-5661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5594-L5661)

## Overview
write_one_nondefault_variable writes a single non-default GUC configuration variable to a binary file for sharing with exec'd backend processes.

## Definition

```c
static void
write_one_nondefault_variable(FILE *fp, struct config_generic *gconf)
```
## Detailed Description
write_one_nondefault_variable is a static helper function that serializes a single PostgreSQL configuration variable to a binary file. This function is part of PostgreSQL's mechanism for sharing non-default GUC settings with newly spawned backend processes through exec.

The function writes data in a specific binary format:
1. Variable name (null-terminated string)
2. Variable value (null-terminated string, formatted according to type)
3. Source file path (null-terminated string, empty if none)
4. Source line number (integer, binary)
5. Variable source (integer, binary)
6. Variable source context (integer, binary)
7. Variable source role OID (binary)

The function handles different variable types with appropriate formatting:
- **Boolean**: Writes "true" or "false"
- **Integer**: Writes decimal representation using %d format
- **Real**: Writes with high precision using %.17g format
- **String**: Writes the string value directly (empty if null)
- **Enum**: Looks up and writes the enum value name

The function includes an assertion to ensure only non-default variables are written (source != PGC_S_DEFAULT).

## Parameters / Member Variables
- `*fp`: File pointer to write the serialized variable data
- `*gconf`: Pointer to the config_generic structure representing the GUC variable to serialize
## Dependencies
- Functions called/Symbols referenced:
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
  - fprintf
  - fputc
  - fwrite
- Called from (representative examples):
  - [write_nondefault_variables](write_nondefault_variables.md)

## Notes and Other Information
- This is a static function only used within guc.c
- Part of the EXEC_BACKEND mechanism for sharing configuration state
- The binary format must match the corresponding read function for proper deserialization
- Real values use %.17g format to maintain precision across serialization
- Source information (file, line, context, role) is preserved for debugging and auditing
- Only variables with source != PGC_S_DEFAULT are written (enforced by assertion)
- Null-terminated strings allow for easy parsing during deserialization

## Simplified Source

```c
// Simplified version of write_one_nondefault_variable
static void write_one_nondefault_variable(FILE *fp, struct config_generic *gconf) {
    // Ensure we only write non-default variables
    Assert(gconf->source != PGC_S_DEFAULT);

    // Write variable name
    fprintf(fp, "%s", gconf->name);
    fputc(0, fp);

    // Write variable value based on type
    switch (gconf->vartype) {
        case PGC_BOOL:
            {
                struct config_bool *conf = (struct config_bool *) gconf;
                fprintf(fp, *conf->variable ? "true" : "false");
            }
            break;

        case PGC_INT:
            {
                struct config_int *conf = (struct config_int *) gconf;
                fprintf(fp, "%d", *conf->variable);
            }
            break;

        case PGC_REAL:
            {
                struct config_real *conf = (struct config_real *) gconf;
                fprintf(fp, "%.17g", *conf->variable);
            }
            break;

        case PGC_STRING:
            {
                struct config_string *conf = (struct config_string *) gconf;
                if (*conf->variable)
                    fprintf(fp, "%s", *conf->variable);
            }
            break;

        case PGC_ENUM:
            {
                struct config_enum *conf = (struct config_enum *) gconf;
                fprintf(fp, "%s", config_enum_lookup_by_value(conf, *conf->variable));
            }
            break;
    }

    // Write null terminator for value
    fputc(0, fp);

    // Write source file (if any)
    if (gconf->sourcefile)
        fprintf(fp, "%s", gconf->sourcefile);
    fputc(0, fp);

    // Write metadata in binary format
    fwrite(&gconf->sourceline, 1, sizeof(gconf->sourceline), fp);
    fwrite(&gconf->source, 1, sizeof(gconf->source), fp);
    fwrite(&gconf->scontext, 1, sizeof(gconf->scontext), fp);
    fwrite(&gconf->srole, 1, sizeof(gconf->srole), fp);
}
```

Key simplifications made:
- Condensed boolean logic using ternary operator for cleaner code
- Maintained all essential logic and functionality
- Preserved the critical assertion and type-specific handling
- Added clear comments explaining each major section
- Kept the precise binary format requirements intact