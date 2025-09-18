# InitializeLogRepWorker

## Location
src/backend/replication/logical/worker.c: 4590 - 4681

## Overview
Common initialization function for all types of logical replication workers (leader apply worker, parallel apply worker, and tablesync worker) that sets up database connection, subscription context, and necessary configuration options.

## Definition


## Detailed Description
This function performs comprehensive initialization for logical replication workers, establishing the runtime environment required for replication processing. The initialization sequence includes:

1. **Security Configuration**: Sets session replication role and secure search path to prevent malicious user code redirection
2. **Database Connection**: Establishes background worker connection to the target database using stored credentials
3. **Memory Management**: Creates persistent ApplyContext for subscription data storage
4. **Subscription Loading**: Loads and validates subscription configuration, ensuring it exists and is enabled
5. **Transaction Safety**: Uses proper locking to prevent concurrent subscription drops during initialization
6. **Configuration Setup**: Applies subscription-specific settings like synchronous_commit behavior
7. **Change Notification**: Registers callbacks for subscription and role changes
8. **Logging**: Provides appropriate startup messages for different worker types

The function handles worker termination gracefully when subscriptions are removed or disabled during startup.

## Parameters / Member Variables
This function takes no parameters but operates on global variables:
- : Global structure containing worker configuration (database ID, user ID, subscription ID)
- : Global subscription object loaded from catalog
- : Memory context created for subscription data
- : Flag indicating successful subscription loading

## Dependencies
- Functions called/Symbols referenced:
  - SetConfigOption: Configure PostgreSQL settings (session_replication_role, search_path, synchronous_commit)
  - BackgroundWorkerInitializeConnectionByOid: Establish database connection
  - AllocSetContextCreate: Create memory context for subscription data
  - StartTransactionCommand/CommitTransactionCommand: Transaction management
  - LockSharedObject: Lock subscription to prevent concurrent drops
  - GetSubscription: Load subscription configuration from catalog
  - am_leader_apply_worker/am_tablesync_worker: Worker type identification
  - ApplyLauncherForgetWorkerStartTime: Clean up launcher tracking
  - apply_worker_exit: Graceful worker termination
  - CacheRegisterSyscacheCallback: Register for catalog change notifications
  - subscription_change_cb: Callback for subscription/role changes
  - get_rel_name: Get table name for logging
- Called from:
  - ParallelApplyWorkerMain: Parallel apply worker initialization
  - SetupApplyOrSyncWorker: Common worker setup path

## Notes and Other Information
- This is a public function that can be called from other source files
- Security is a primary concern - sets secure search path and replication role to prevent privilege escalation
- Handles subscription lifecycle carefully with proper locking to avoid race conditions
- Memory context management ensures subscription data persists for worker lifetime
- Different log messages are generated based on worker type (apply vs tablesync)
- Worker termination is handled gracefully with proper cleanup when subscriptions become unavailable
- Registers for dynamic configuration changes to handle subscription modifications during runtime