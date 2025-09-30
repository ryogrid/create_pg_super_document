# parse_subscription_options

## Location
[src/backend/commands/subscriptioncmds.c:121-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L121-L454)

## Overview
Parses and validates subscription options for CREATE and ALTER SUBSCRIPTION commands, handling option conflicts and setting appropriate defaults based on supported option flags.

## Definition

```c
static void
parse_subscription_options(ParseState *pstate, List *stmt_options,
						   bits32 supported_opts, SubOpts *opts)
```
## Detailed Description
This function serves as a common option parsing routine for both CREATE and ALTER SUBSCRIPTION commands. It takes a list of user-provided subscription options and processes them against a bitmask of supported options, populating a SubOpts structure with the parsed values. The function enforces mutual exclusivity rules between certain options and validates option values to prevent invalid subscription configurations.

The function performs several key tasks:
- Sets default values for all supported options based on the supported_opts bitmask
- Iterates through the provided statement options and parses each one
- Validates option values and enforces constraints (e.g., replication slot name validation)
- Checks for conflicting options and reports errors when incompatible combinations are specified
- Handles special cases like "connect = false" which affects other option defaults
- Ensures "slot_name = NONE" is properly handled with related option constraints

The function supports a comprehensive set of subscription options including connection settings, replication slot management, data copying behavior, streaming options, and various behavioral flags.

## Parameters / Member Variables
- : ParseState context for error reporting and parsing state management
- : List of DefElem structures containing user-specified subscription options
- : Bitmask indicating which subscription options are valid for this command context
- : Output parameter - SubOpts structure to be populated with parsed option values

## Dependencies
- Functions called/Symbols referenced:
  - [defGetBoolean](../d/defGetBoolean.md): Extracts boolean values from DefElem structures
  - [defGetString](../d/defGetString.md): Extracts string values from DefElem structures  
  - [defGetStreamingMode](../d/defGetStreamingMode.md): Parses streaming mode values
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md): Reports conflicts when same option specified multiple times
  - [ReplicationSlotValidateName](../R/ReplicationSlotValidateName.md): Validates replication slot names
  - [set_config_option](../s/set_config_option.md): Tests validity of synchronous_commit values
  - IsSet: Macro to test if option bit is set in bitmask
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md): During subscription creation
  - [AlterSubscription](../A/AlterSubscription.md): During subscription modification operations

## Notes and Other Information
- The function enforces that "connect = false" is incompatible with "enabled = true", "create_slot = true", and "copy_data = true"
- Special handling exists for "two_phase" option which cannot be toggled in ALTER operations to prevent transaction inconsistencies
- "slot_name = NONE" requires "enabled = false" and "create_slot = false" to prevent invalid subscription states
- The function validates LSN format and rejects invalid WAL location specifications
- Origin parameter currently supports only "none" and "any" values but is designed for future extensibility

## Simplified Source

```c
static void
parse_subscription_options(ParseState *pstate, List *stmt_options,
                           bits32 supported_opts, SubOpts *opts)
{
    ListCell *lc;

    // Initialize output structure with defaults
    memset(opts, 0, sizeof(SubOpts));

    // Set defaults for supported options
    if (IsSet(supported_opts, SUBOPT_CONNECT)) opts->connect = true;
    if (IsSet(supported_opts, SUBOPT_ENABLED)) opts->enabled = true;
    if (IsSet(supported_opts, SUBOPT_CREATE_SLOT)) opts->create_slot = true;
    if (IsSet(supported_opts, SUBOPT_COPY_DATA)) opts->copy_data = true;
    if (IsSet(supported_opts, SUBOPT_BINARY)) opts->binary = false;
    if (IsSet(supported_opts, SUBOPT_STREAMING)) opts->streaming = LOGICALREP_STREAM_OFF;
    // ... other defaults ...

    // Parse each user-provided option
    foreach(lc, stmt_options) {
        DefElem *defel = (DefElem *) lfirst(lc);

        // Check for duplicate option specifications
        if (option_already_specified(opts, defel->defname)) {
            errorConflictingDefElem(defel, pstate);
        }

        // Parse specific options
        if (strcmp(defel->defname, "connect") == 0) {
            opts->connect = defGetBoolean(defel);
            opts->specified_opts |= SUBOPT_CONNECT;
        }
        else if (strcmp(defel->defname, "enabled") == 0) {
            opts->enabled = defGetBoolean(defel);
            opts->specified_opts |= SUBOPT_ENABLED;
        }
        else if (strcmp(defel->defname, "slot_name") == 0) {
            opts->slot_name = defGetString(defel);
            // Handle "none" as NULL slot name
            if (strcmp(opts->slot_name, "none") == 0)
                opts->slot_name = NULL;
            else
                ReplicationSlotValidateName(opts->slot_name, ERROR);
            opts->specified_opts |= SUBOPT_SLOT_NAME;
        }
        // ... handle other options (binary, streaming, etc.) ...
        else {
            ereport(ERROR, "unrecognized subscription parameter");
        }
    }

    // Validate option combinations and handle conflicts
    validate_option_combinations(opts, supported_opts);
}
```