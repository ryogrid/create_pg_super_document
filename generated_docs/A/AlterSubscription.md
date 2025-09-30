# AlterSubscription

## Location
[src/backend/commands/subscriptioncmds.c:1084-1552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L1084-L1552)

## Overview
AlterSubscription modifies existing logical replication subscriptions, handling various types of changes including options, connection settings, publications, and enabling/disabling subscriptions.

## Definition

```c
ObjectAddress
AlterSubscription(ParseState *pstate, AlterSubscriptionStmt *stmt,
				  bool isTopLevel)
```
## Detailed Description
AlterSubscription is the main function for handling ALTER SUBSCRIPTION SQL commands in PostgreSQL's logical replication system. It performs comprehensive validation and modification of subscription properties stored in the pg_subscription system catalog.

The function handles multiple alteration types through a switch statement:
- ALTER_SUBSCRIPTION_OPTIONS: Modifies subscription options like slot_name, binary, streaming, failover, etc.
- ALTER_SUBSCRIPTION_ENABLED: Enables or disables the subscription
- ALTER_SUBSCRIPTION_CONNECTION: Updates connection string to the publisher
- ALTER_SUBSCRIPTION_SET_PUBLICATION: Changes the list of publications
- ALTER_SUBSCRIPTION_ADD_PUBLICATION/DROP_PUBLICATION: Adds or removes individual publications
- ALTER_SUBSCRIPTION_REFRESH: Refreshes subscription table list from publisher
- ALTER_SUBSCRIPTION_SKIP: Sets LSN to skip problematic transactions

The function enforces security restrictions, preventing non-superusers from modifying subscriptions with password_required=false. It also validates state dependencies, such as preventing certain operations on enabled subscriptions and ensuring proper transaction block handling for operations that cannot be rolled back.

## Parameters / Member Variables
- : Parser state for processing subscription options and validating syntax
- : ALTER SUBSCRIPTION statement containing the specific alteration type and parameters
- : Boolean flag indicating if this is a top-level command, used for transaction block validation

## Dependencies
- Functions called/Symbols referenced:
  - [GetSubscription](../G/GetSubscription.md): Retrieves subscription details from catalog
  - [parse_subscription_options](../p/parse_subscription_options.md): Parses and validates subscription option changes
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md): Prevents operations that can't be rolled back from running in transaction blocks
  - [publicationListToArray](../p/publicationListToArray.md): Converts publication name list to array format
  - [AlterSubscription_refresh](AlterSubscription_refresh.md): Handles subscription refresh operations
  - walrcv_alter_slot: Alters replication slot properties on publisher
  - [heap_freetuple](../h/heap_freetuple.md): Frees heap tuple memory
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processor in tcop/utility.c:1863

## Notes and Other Information
- Requires exclusive lock on the subscription to prevent concurrent modifications
- Enforces ownership checks and superuser restrictions for security-sensitive options
- Handles complex state validations for two-phase commit and failover scenarios
- Some operations like failover changes cannot be rolled back and must be prevented in transaction blocks
- Automatically wakes up replication workers when changes require immediate processing
- Connection to publisher is established only when necessary (e.g., for slot alteration)

## Simplified Source

```c
ObjectAddress AlterSubscription(ParseState *pstate, AlterSubscriptionStmt *stmt, bool isTopLevel) {
    Relation rel;
    ObjectAddress myself;
    HeapTuple tup;
    Oid subid;
    bool update_tuple = false;
    Subscription *sub;
    SubOpts opts = {0};

    // Open subscription catalog and find existing subscription
    rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, MyDatabaseId, CStringGetDatum(stmt->subname));

    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("subscription \"%s\" does not exist", stmt->subname)));
    }

    // Get subscription info and check permissions
    subid = ((Form_pg_subscription) GETSTRUCT(tup))->oid;
    if (!object_ownercheck(SubscriptionRelationId, subid, GetUserId())) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SUBSCRIPTION, stmt->subname);
    }

    sub = GetSubscription(subid, false);

    // Check superuser requirements for password_required=false
    if (!sub->passwordrequired && !superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("password_required=false is superuser-only")));
    }

    // Lock subscription for exclusive access
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    // Handle different alteration types
    switch (stmt->kind) {
        case ALTER_SUBSCRIPTION_OPTIONS:
            // Parse and apply subscription option changes
            parse_subscription_options(pstate, stmt->options, supported_opts, &opts);
            // Update individual options like slot_name, binary, streaming, etc.
            update_tuple = true;
            break;

        case ALTER_SUBSCRIPTION_ENABLED:
            // Enable or disable subscription
            parse_subscription_options(pstate, stmt->options, SUBOPT_ENABLED, &opts);
            if (!sub->slotname && opts.enabled) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot enable subscription that does not have a slot name")));
            }
            if (opts.enabled) {
                ApplyLauncherWakeupAtCommit();
            }
            update_tuple = true;
            break;

        case ALTER_SUBSCRIPTION_CONNECTION:
            // Update connection string
            load_file("libpqwalreceiver", false);
            walrcv_check_conninfo(stmt->conninfo, sub->passwordrequired && !sub->ownersuperuser);
            update_tuple = true;
            break;

        case ALTER_SUBSCRIPTION_SET_PUBLICATION:
            // Replace publication list
            parse_subscription_options(pstate, stmt->options, supported_opts, &opts);
            if (opts.refresh) {
                if (!sub->enabled) {
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions")));
                }
                PreventInTransactionBlock(isTopLevel, "ALTER SUBSCRIPTION with refresh");
                AlterSubscription_refresh(sub, opts.copy_data, stmt->publication);
            }
            update_tuple = true;
            break;

        case ALTER_SUBSCRIPTION_ADD_PUBLICATION:
        case ALTER_SUBSCRIPTION_DROP_PUBLICATION:
            // Add or remove publications from list
            parse_subscription_options(pstate, stmt->options, supported_opts, &opts);
            if (opts.refresh) {
                PreventInTransactionBlock(isTopLevel, "ALTER SUBSCRIPTION with refresh");
                AlterSubscription_refresh(sub, opts.copy_data, validate_publications);
            }
            update_tuple = true;
            break;

        case ALTER_SUBSCRIPTION_REFRESH:
            // Refresh subscription table list
            if (!sub->enabled) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions")));
            }
            PreventInTransactionBlock(isTopLevel, "ALTER SUBSCRIPTION ... REFRESH");
            AlterSubscription_refresh(sub, opts.copy_data, NULL);
            break;

        case ALTER_SUBSCRIPTION_SKIP:
            // Set LSN to skip problematic transactions
            parse_subscription_options(pstate, stmt->options, SUBOPT_LSN, &opts);
            // Validate LSN is reasonable
            update_tuple = true;
            break;
    }

    // Update catalog if changes were made
    if (update_tuple) {
        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
        CatalogTupleUpdate(rel, &tup->t_self, tup);
        heap_freetuple(tup);
    }

    // Handle failover slot changes (requires publisher connection)
    if (replaces[Anum_pg_subscription_subfailover - 1]) {
        // Connect to publisher and alter slot
        WalReceiverConn *wrconn = walrcv_connect(sub->conninfo, true, true, must_use_password, sub->name, &err);
        if (!wrconn) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("could not connect to the publisher: %s", err)));
        }
        walrcv_alter_slot(wrconn, sub->slotname, opts.failover);
        walrcv_disconnect(wrconn);
    }

    table_close(rel, RowExclusiveLock);
    ObjectAddressSet(myself, SubscriptionRelationId, subid);

    // Wake up workers and invoke hooks
    InvokeObjectPostAlterHook(SubscriptionRelationId, subid, 0);
    LogicalRepWorkersWakeupAtCommit(subid);

    return myself;
}
```