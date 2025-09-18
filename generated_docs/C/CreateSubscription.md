# CreateSubscription

## Location
src/backend/commands/subscriptioncmds.c: 579 - 858

## Overview
Creates a new logical replication subscription, establishing the necessary catalog entries, replication slot, and table synchronization states to replicate data from a publisher database.

## Definition


## Detailed Description
This function implements the CREATE SUBSCRIPTION SQL command, which establishes a logical replication subscription to replicate data from a remote PostgreSQL publisher. The function performs comprehensive validation, creates system catalog entries, and optionally establishes the replication infrastructure.

Key operations performed:
1. **Option Parsing and Validation**: Processes subscription options using parse_subscription_options and validates compatibility
2. **Permission Checks**: Ensures the user has appropriate privileges (pg_create_subscription role, database CREATE permission)
3. **Security Validation**: Enforces password requirements for non-superusers and connection string validation  
4. **Catalog Management**: Creates the subscription entry in pg_subscription with all specified attributes
5. **Replication Infrastructure**: Establishes replication origin for conflict resolution tracking
6. **Publisher Communication**: Connects to publisher to validate publications and retrieve table information
7. **Table State Initialization**: Sets up initial synchronization state for all published tables
8. **Slot Creation**: Optionally creates replication slot on publisher with appropriate two-phase settings
9. **Process Management**: Triggers apply launcher if subscription is enabled

The function uses a comprehensive transaction-safe approach with proper cleanup via PG_TRY/PG_FINALLY blocks to handle connection failures gracefully.

## Parameters / Member Variables
- : Parse state context for error reporting and SQL parsing information
- : CreateSubscriptionStmt containing subscription name, connection info, publications, and options
- : Boolean indicating if command is executed at top transaction level (affects transaction block restrictions)

## Dependencies
- Functions called/Symbols referenced:
  - parse_subscription_options: Parses and validates subscription creation options
  - PreventInTransactionBlock: Prevents execution in transaction block when creating replication slots
  - has_privs_of_role: Checks if user has pg_create_subscription privileges
  - walrcv_check_conninfo/walrcv_connect: Validates and establishes publisher connection
  - check_publications: Validates publication existence on publisher
  - publicationListToArray: Converts publication list to array for catalog storage
  - recordDependencyOnOwner: Records ownership dependency in system catalogs
  - AddSubscriptionRelState: Initializes table synchronization states
  - replorigin_create: Creates replication origin for conflict tracking
- Called from (representative examples):
  - ProcessUtilitySlow: During SQL command processing for CREATE SUBSCRIPTION statements

## Notes and Other Information
- Requires pg_create_subscription role membership for security (prevents arbitrary network access)
- Non-superusers must set password_required=true unless exempted by superuser
- Creating replication slots prevents execution in transaction blocks due to non-transactional nature
- Two-phase commit support is conditionally enabled based on copy_data and table readiness states
- Automatically creates replication origin with standardized naming convention for conflict resolution
- Table synchronization states are set to INIT (requires copy) or READY (no copy needed) based on copy_data option
- Connection validation includes libpqwalreceiver library loading for publisher communication
- Supports comprehensive regression testing name validation when compiled with appropriate flags
- Triggers apply launcher process when subscription is created in enabled state