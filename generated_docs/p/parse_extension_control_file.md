# parse_extension_control_file

## Location
[src/backend/commands/extension.c:476-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L476-L647)

## Overview
Parses the contents of primary or auxiliary extension control files and populates an ExtensionControlFile structure with configuration parameters.

## Definition
```c
static void parse_extension_control_file(ExtensionControlFile *control, const char *version)
```

## Detailed Description
This function reads and parses extension control files, which contain configuration parameters that define how PostgreSQL extensions behave. It handles two types of control files:
1. Primary control files (when version is NULL): Contains main extension configuration
2. Auxiliary control files (when version is provided): Contains version-specific overrides

The function uses PostgreSQL's configuration file parsing infrastructure to read key-value pairs and validates each parameter according to extension control file specifications. It supports various extension parameters including directory, default_version, module_pathname, comment, schema, relocatable, superuser, trusted, encoding, requires, and no_relocate.

The function enforces rules about which parameters can appear in auxiliary files versus primary files, and validates parameter values (e.g., boolean validation, encoding validation, identifier list parsing).

## Parameters / Member Variables
- `control`: Pointer to ExtensionControlFile structure to populate with parsed values
- `version`: Version string for auxiliary file parsing, or NULL for primary control file

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_aux_control_filename](../g/get_extension_aux_control_filename.md)
  - [get_extension_control_filename](../g/get_extension_control_filename.md)
  - [AllocateFile](../A/AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)
  - ParseConfigFp
  - FreeConfigVariables
  - [pstrdup](pstrdup.md)
  - [parse_bool](parse_bool.md)
  - [pg_valid_server_encoding](pg_valid_server_encoding.md)
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - ereport
- Types referenced:
  - [ExtensionControlFile](../E/ExtensionControlFile.md)
  - [ConfigVariable](../C/ConfigVariable.md)
- Called from (representative examples):
  - [read_extension_control_file](../r/read_extension_control_file.md)
  - [read_extension_aux_control_file](../r/read_extension_aux_control_file.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the extension.c source file
- Control files are expected to be small (half dozen lines) and ASCII-encoded
- Auxiliary files are optional - missing auxiliary files do not generate errors
- Missing primary control files indicate the extension is not installed and generate appropriate error messages
- Enforces mutual exclusivity between relocatable=true and fixed schema specifications
- Uses PostgreSQL's standard configuration file parsing with GUC infrastructure
- Provides detailed error messages for invalid parameters and values
- Validates boolean parameters, encoding names, and identifier lists
- Some parameters (directory, default_version) are prohibited in auxiliary control files

## Simplified Source

```c
static void
parse_extension_control_file(ExtensionControlFile *control, const char *version)
{
    char *filename;
    FILE *file;
    ConfigVariable *item, *head = NULL, *tail = NULL;

    // Get filename for primary or auxiliary control file
    if (version)
        filename = get_extension_aux_control_filename(control, version);
    else
        filename = get_extension_control_filename(control->name);

    // Open file (auxiliary files are optional)
    if ((file = AllocateFile(filename, "r")) == NULL) {
        if (errno == ENOENT) {
            if (version) {
                pfree(filename);
                return;  // Missing auxiliary file is OK
            }
            ereport(ERROR, (errmsg("extension \"%s\" is not available", control->name)));
        }
        ereport(ERROR, (errmsg("could not open extension control file")));
    }

    // Parse file content using GUC parsing infrastructure
    ParseConfigFp(file, filename, CONF_FILE_START_DEPTH, ERROR, &head, &tail);
    FreeFile(file);

    // Process each configuration parameter
    for (item = head; item != NULL; item = item->next) {
        if (strcmp(item->name, "directory") == 0) {
            if (version) ereport(ERROR, (errmsg("parameter cannot be set in auxiliary file")));
            control->directory = pstrdup(item->value);
        }
        else if (strcmp(item->name, "default_version") == 0) {
            if (version) ereport(ERROR, (errmsg("parameter cannot be set in auxiliary file")));
            control->default_version = pstrdup(item->value);
        }
        else if (strcmp(item->name, "module_pathname") == 0) {
            control->module_pathname = pstrdup(item->value);
        }
        else if (strcmp(item->name, "comment") == 0) {
            control->comment = pstrdup(item->value);
        }
        else if (strcmp(item->name, "schema") == 0) {
            control->schema = pstrdup(item->value);
        }
        else if (strcmp(item->name, "relocatable") == 0) {
            if (!parse_bool(item->value, &control->relocatable))
                ereport(ERROR, (errmsg("parameter requires Boolean value")));
        }
        else if (strcmp(item->name, "superuser") == 0) {
            if (!parse_bool(item->value, &control->superuser))
                ereport(ERROR, (errmsg("parameter requires Boolean value")));
        }
        else if (strcmp(item->name, "trusted") == 0) {
            if (!parse_bool(item->value, &control->trusted))
                ereport(ERROR, (errmsg("parameter requires Boolean value")));
        }
        else if (strcmp(item->name, "encoding") == 0) {
            control->encoding = pg_valid_server_encoding(item->value);
            if (control->encoding < 0)
                ereport(ERROR, (errmsg("invalid encoding name")));
        }
        else if (strcmp(item->name, "requires") == 0) {
            char *rawnames = pstrdup(item->value);
            if (!SplitIdentifierString(rawnames, ',', &control->requires))
                ereport(ERROR, (errmsg("must be list of extension names")));
        }
        else if (strcmp(item->name, "no_relocate") == 0) {
            char *rawnames = pstrdup(item->value);
            if (!SplitIdentifierString(rawnames, ',', &control->no_relocate))
                ereport(ERROR, (errmsg("must be list of extension names")));
        }
        else {
            ereport(ERROR, (errmsg("unrecognized parameter \"%s\"", item->name)));
        }
    }

    FreeConfigVariables(head);

    // Validate parameter combinations
    if (control->relocatable && control->schema != NULL)
        ereport(ERROR, (errmsg("cannot specify schema when relocatable is true")));

    pfree(filename);
}
```