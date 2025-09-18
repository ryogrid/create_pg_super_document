# parse_subscription_options

## Location
src/backend/commands/subscriptioncmds.c: 121 - 454

## Overview
Parses and validates subscription options for CREATE and ALTER SUBSCRIPTION commands, handling option conflicts and setting appropriate defaults based on supported option flags.

## Definition


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
  - defGetBoolean: Extracts boolean values from DefElem structures
  - defGetString: Extracts string values from DefElem structures  
  - defGetStreamingMode: Parses streaming mode values
  - errorConflictingDefElem: Reports conflicts when same option specified multiple times
  - ReplicationSlotValidateName: Validates replication slot names
  - set_config_option: Tests validity of synchronous_commit values
  - IsSet: Macro to test if option bit is set in bitmask
- Called from (representative examples):
  - CreateSubscription: During subscription creation
  - AlterSubscription: During subscription modification operations

## Notes and Other Information
- The function enforces that "connect = false" is incompatible with "enabled = true", "create_slot = true", and "copy_data = true"
- Special handling exists for "two_phase" option which cannot be toggled in ALTER operations to prevent transaction inconsistencies
- "slot_name = NONE" requires "enabled = false" and "create_slot = false" to prevent invalid subscription states
- The function validates LSN format and rejects invalid WAL location specifications
- Origin parameter currently supports only "none" and "any" values but is designed for future extensibility