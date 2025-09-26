# store_pub_sub_info

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 433 - 504

## Overview
Initializes and stores publication and subscription information for each database, creating LogicalRepInfo structures that contain connection details and object names for both publisher and subscriber sides.

## Definition


## Detailed Description
The  function is responsible for setting up the core data structures that track logical replication information for each database involved in the subscription creation process. It creates an array of  structures, one for each database specified in the options.

The function handles both user-specified names and automatic name generation scenarios. If publication names, subscription names, or replication slot names are provided in the options, it assigns them to the corresponding databases. Otherwise, these fields are left NULL and will be assigned generated names later in the  function.

For each database, the function creates complete connection strings by combining the base connection information with the specific database name using the  helper function. This ensures that both publisher and subscriber connections are properly configured for each target database.

## Parameters
- : Pointer to CreateSubscriberOptions structure containing user-specified configuration including database names, publication names, subscription names, and replication slot names
- : Base connection string for the publisher (without database name)
- : Base connection string for the subscriber (without database name)

## Dependencies
- Functions called/Symbols referenced:
  -  - Allocates memory for the array of LogicalRepInfo structures
  -  - Creates complete connection strings by appending database names
  -  - Logs debugging information about the configured publisher and subscriber details
- Structures referenced:
  -  - Input configuration structure
  -  - Main data structure for tracking replication information
  -  - Used for iterating through option lists
- Called from:
  -  structure initialization
  -  function for setting up replication information

## Notes and Other Information
- The function is marked as , indicating it's only used within the pg_createsubscriber.c file
- Returns a dynamically allocated array that must be freed by the caller
- Handles mismatched list lengths gracefully - if fewer names are provided than databases, remaining entries get NULL (auto-generated names)
- The  and  flags are initialized to false and will be set during actual object creation
- Debug logging provides visibility into both publisher and subscriber configurations for troubleshooting
- The function assumes global variables , , , and  are properly initialized
- Connection strings are created for both publisher and subscriber sides, even though they may point to the same server initially