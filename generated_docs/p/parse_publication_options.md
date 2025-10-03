# parse_publication_options

## Location
[src/backend/commands/publicationcmds.c:76-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L76-L165)

## Overview
Parses publication options from a list of DefElem nodes, setting publication actions and partition root publishing preferences with validation and error handling.

## Definition

```c
static void
parse_publication_options(ParseState *pstate,
						  List *options,
						  bool *publish_given,
						  PublicationActions *pubactions,
						  bool *publish_via_partition_root_given,
						  bool *publish_via_partition_root)
```
## Detailed Description
This function processes publication creation or alteration options by parsing a list of DefElem nodes. It handles two main publication parameters: the 'publish' option that controls which DML operations (insert, update, delete, truncate) are replicated, and the 'publish_via_partition_root' option that controls whether changes to partitioned tables are published as coming from the partition or the root table.

The function first sets default values for all publication actions (all enabled by default) and then processes the options list. For the 'publish' parameter, it parses a comma-separated list of operation names and enables only those explicitly specified. The function includes comprehensive error checking for duplicate options, invalid syntax, and unrecognized parameter names.

## Parameters / Member Variables
- `*pstate`: ParseState context for error reporting and parsing operations
- `*options`: List of DefElem nodes containing the publication options to parse
- `*publish_given`: Output parameter indicating whether the publish option was explicitly provided
- `*pubactions`: Output structure containing boolean flags for each publication action (insert, update, delete, truncate)
- `*publish_via_partition_root_given`: Output parameter indicating whether the partition root option was explicitly provided
- `*publish_via_partition_root`: Output parameter indicating whether to publish via partition root
## Dependencies
- Functions called/Symbols referenced:
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md)
  - [defGetString](../d/defGetString.md)
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - [defGetBoolean](../d/defGetBoolean.md)
  - [PublicationActions](../P/PublicationActions.md)
  - [DefElem](../D/DefElem.md)
- Called from (representative examples):
  - [CreatePublication](../C/CreatePublication.md)
  - [AlterPublicationOptions](../A/AlterPublicationOptions.md)

## Notes and Other Information
- Sets default publication actions to true for all DML operations (insert, update, delete, truncate)
- When 'publish' option is provided, it first disables all actions then enables only those explicitly listed
- Validates that each option is specified at most once to prevent conflicts
- Supports parsing comma-separated lists for the 'publish' parameter
- Provides detailed error messages for invalid syntax and unrecognized options
- Located in src/backend/commands/publicationcmds.c:76-165

## Simplified Source

```c
static void parse_publication_options(ParseState *pstate,
                                     List *options,
                                     bool *publish_given,
                                     PublicationActions *pubactions,
                                     bool *publish_via_partition_root_given,
                                     bool *publish_via_partition_root)
{
    ListCell *lc;

    *publish_given = false;
    *publish_via_partition_root_given = false;

    // Set defaults - all DML operations enabled by default
    pubactions->pubinsert = true;
    pubactions->pubupdate = true;
    pubactions->pubdelete = true;
    pubactions->pubtruncate = true;
    *publish_via_partition_root = false;

    // Parse each option
    foreach(lc, options) {
        DefElem *defel = (DefElem *) lfirst(lc);

        if (strcmp(defel->defname, "publish") == 0) {
            char *publish;
            List *publish_list;
            ListCell *lc2;

            if (*publish_given) {
                errorConflictingDefElem(defel, pstate);
            }

            // When publish option is given, only explicitly listed actions are enabled
            pubactions->pubinsert = false;
            pubactions->pubupdate = false;
            pubactions->pubdelete = false;
            pubactions->pubtruncate = false;

            *publish_given = true;
            publish = defGetString(defel);

            if (!SplitIdentifierString(publish, ',', &publish_list)) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("invalid list syntax in parameter \"%s\"", "publish")));
            }

            // Process each publish option
            foreach(lc2, publish_list) {
                char *publish_opt = (char *) lfirst(lc2);

                if (strcmp(publish_opt, "insert") == 0)
                    pubactions->pubinsert = true;
                else if (strcmp(publish_opt, "update") == 0)
                    pubactions->pubupdate = true;
                else if (strcmp(publish_opt, "delete") == 0)
                    pubactions->pubdelete = true;
                else if (strcmp(publish_opt, "truncate") == 0)
                    pubactions->pubtruncate = true;
                else
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("unrecognized value for publication option \"%s\": \"%s\"",
                                    "publish", publish_opt)));
            }
        }
        else if (strcmp(defel->defname, "publish_via_partition_root") == 0) {
            if (*publish_via_partition_root_given) {
                errorConflictingDefElem(defel, pstate);
            }
            *publish_via_partition_root_given = true;
            *publish_via_partition_root = defGetBoolean(defel);
        }
        else {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("unrecognized publication parameter: \"%s\"", defel->defname)));
        }
    }
}
```