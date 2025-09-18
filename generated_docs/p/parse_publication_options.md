# parse_publication_options

## Location
src/backend/commands/publicationcmds.c: 76 - 165

## Overview
Parses publication options from a list of DefElem nodes, setting publication actions and partition root publishing preferences with validation and error handling.

## Definition


## Detailed Description
This function processes publication creation or alteration options by parsing a list of DefElem nodes. It handles two main publication parameters: the 'publish' option that controls which DML operations (insert, update, delete, truncate) are replicated, and the 'publish_via_partition_root' option that controls whether changes to partitioned tables are published as coming from the partition or the root table.

The function first sets default values for all publication actions (all enabled by default) and then processes the options list. For the 'publish' parameter, it parses a comma-separated list of operation names and enables only those explicitly specified. The function includes comprehensive error checking for duplicate options, invalid syntax, and unrecognized parameter names.

## Parameters / Member Variables
- : ParseState context for error reporting and parsing operations
- : List of DefElem nodes containing the publication options to parse
- : Output parameter indicating whether the publish option was explicitly provided
- : Output structure containing boolean flags for each publication action (insert, update, delete, truncate)
- : Output parameter indicating whether the partition root option was explicitly provided
- : Output parameter indicating whether to publish via partition root

## Dependencies
- Functions called/Symbols referenced:
  - errorConflictingDefElem
  - defGetString
  - SplitIdentifierString
  - defGetBoolean
  - PublicationActions
  - DefElem
- Called from (representative examples):
  - CreatePublication
  - AlterPublicationOptions

## Notes and Other Information
- Sets default publication actions to true for all DML operations (insert, update, delete, truncate)
- When 'publish' option is provided, it first disables all actions then enables only those explicitly listed
- Validates that each option is specified at most once to prevent conflicts
- Supports parsing comma-separated lists for the 'publish' parameter
- Provides detailed error messages for invalid syntax and unrecognized options
- Located in src/backend/commands/publicationcmds.c:76-165