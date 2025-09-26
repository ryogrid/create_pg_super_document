# ConfigVariable

## Location
[src/include/utils/guc.h:135-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc.h#L135-L145)

## Overview
ConfigVariable is a structure used to represent configuration file entries during parsing and processing, including name-value pairs with source location information and error handling capabilities.

## Definition

```c
typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *errmsg;
	char	   *filename;
	int			sourceline;
	bool		ignore;
	bool		applied;
	struct ConfigVariable *next;
} ConfigVariable;
```
## Detailed Description
The ConfigVariable structure serves as the primary data container for configuration file parsing in PostgreSQL's GUC (Grand Unified Configuration) system. When configuration files are parsed, they return a list of these structures containing name-value pairs along with metadata about their source location. 

This structure is also used to carry error reports about configuration files. When an entry represents an error, the errmsg field will be non-NULL, and the name, value, and/or filename fields might be NULL.

The structure supports a linked list implementation through the next pointer, allowing multiple configuration variables to be chained together during processing.

## Parameters / Member Variables
- : The configuration parameter name (e.g., "shared_buffers")
- : The configuration parameter value (e.g., "128MB")
- : Error message if there was a problem parsing this entry (NULL for successful entries)
- : Name of the configuration file where this entry was found
- : Line number in the configuration file where this entry appears
- : Flag indicating whether to skip processing this item (true for errors or duplicate items)
- : Flag set to true if the setting was successfully applied or could have been applied
- : Pointer to the next ConfigVariable in a linked list

## Dependencies
- Functions called/Symbols referenced:
  - [ConfigVariable](ConfigVariable.md) (self-reference for linked list structure)
- Called from (representative examples):
  - [parse_extension_control_file](../p/parse_extension_control_file.md)
  - [write_auto_conf_file](../w/write_auto_conf_file.md)
  - [replace_auto_config_value](../r/replace_auto_config_value.md)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md)
  - [GUCHashEntry](../G/GUCHashEntry.md) (uses ConfigVariable members)

## Notes and Other Information
- This structure is dual-purpose: it carries both successful configuration entries and error reports
- The linked list design allows efficient processing of multiple configuration entries
- Error entries may have NULL values for some fields, requiring careful null-checking in processing code
- The ignore and applied flags provide fine-grained control over configuration processing workflow
- Source location information (filename and sourceline) enables precise error reporting and debugging