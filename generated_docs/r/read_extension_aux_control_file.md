# read_extension_aux_control_file

## Location
src/backend/commands/extension.c: 677 - 699

## Overview
Reads an auxiliary control file for a specific extension version and returns a new ExtensionControlFile structure with version-specific parameter overrides applied to the primary configuration.

## Definition
```c
static ExtensionControlFile *read_extension_aux_control_file(const ExtensionControlFile *pcontrol, const char *version)
```

## Detailed Description
This function creates a version-specific extension control configuration by reading an auxiliary control file and overlaying its parameters on top of the primary control file configuration. It performs a shallow copy of the primary control structure, meaning pointer fields are shared between the original and new structures, then parses the auxiliary control file to override specific parameters for the given version.

Auxiliary control files allow extensions to specify version-specific configuration that differs from the primary control file. For example, a newer version might have different module_pathname, comment, or dependency requirements while maintaining the same core extension properties.

The function preserves the original primary control structure unchanged and returns a new structure reflecting the combined configuration of primary + auxiliary control file parameters.

## Parameters / Member Variables
- `pcontrol`: Pointer to the primary ExtensionControlFile structure containing base configuration
- `version`: Version string specifying which auxiliary control file to read

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - memcpy
  - parse_extension_control_file
- Types referenced:
  - ExtensionControlFile
- Called from (representative examples):
  - CreateExtensionInternal
  - get_available_versions_for_extension
  - ApplyExtensionUpdates

## Notes and Other Information
- This is a static function, meaning it's only accessible within the extension.c source file
- Uses shallow copying (memcpy) so pointer fields are shared between primary and auxiliary control structures
- Auxiliary control files are optional - if the file doesn't exist, the function returns the primary configuration unchanged
- The returned structure must be freed by the caller
- Only certain parameters can be overridden in auxiliary control files (as enforced by parse_extension_control_file)
- Parameters like directory and default_version cannot be specified in auxiliary files
- The function enables version-specific customization of extension behavior while maintaining core extension identity