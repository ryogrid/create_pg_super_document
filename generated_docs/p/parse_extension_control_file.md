# parse_extension_control_file

## Location
src/backend/commands/extension.c: 476 - 647

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
  - get_extension_aux_control_filename
  - get_extension_control_filename
  - AllocateFile
  - FreeFile
  - ParseConfigFp
  - FreeConfigVariables
  - pstrdup
  - parse_bool
  - pg_valid_server_encoding
  - SplitIdentifierString
  - ereport
- Types referenced:
  - ExtensionControlFile
  - ConfigVariable
- Called from (representative examples):
  - read_extension_control_file
  - read_extension_aux_control_file

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