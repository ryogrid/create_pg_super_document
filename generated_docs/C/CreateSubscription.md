# CreateSubscription

## Location
[src/backend/commands/subscriptioncmds.c:579-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L579-L858)

## Overview
Creates a new logical replication subscription, establishing the necessary catalog entries, replication slot, and table synchronization states to replicate data from a publisher database.

## Definition

```c
typedef struct SubRemoveRels
	{
		Oid			relid;
		char		state;
	} SubRemoveRels;
```
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

## Parameters
- `pstate`: Parse state context for error reporting and SQL parsing information
- `stmt`: CreateSubscriptionStmt containing subscription name, connection info, publications, and options
- `isTopLevel`: Boolean indicating if command is executed at top transaction level (affects transaction block restrictions)

## Dependencies
- Functions called/Symbols referenced:
  - [parse_subscription_options](../p/parse_subscription_options.md): Parses and validates subscription creation options
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md): Prevents execution in transaction block when creating replication slots
  - [has_privs_of_role](../h/has_privs_of_role.md): Checks if user has pg_create_subscription privileges
  - walrcv_check_conninfo/walrcv_connect: Validates and establishes publisher connection
  - [check_publications](../c/check_publications.md): Validates publication existence on publisher
  - [publicationListToArray](../p/publicationListToArray.md): Converts publication list to array for catalog storage
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md): Records ownership dependency in system catalogs
  - [AddSubscriptionRelState](../A/AddSubscriptionRelState.md): Initializes table synchronization states
  - [replorigin_create](../r/replorigin_create.md): Creates replication origin for conflict tracking
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): During SQL command processing for CREATE SUBSCRIPTION statements

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

## Simplified Source

```c
ObjectAddress CreateSubscription(ParseState *pstate, CreateSubscriptionStmt *stmt,
                               bool isTopLevel)
{
    Oid subid, owner = GetUserId();
    Datum values[Natts_pg_subscription];
    bool nulls[Natts_pg_subscription];
    HeapTuple tup;
    char *conninfo = stmt->conninfo;
    List *publications = stmt->publication;
    SubOpts opts = {0};

    // Parse and validate subscription options
    bits32 supported_opts = (SUBOPT_CONNECT | SUBOPT_ENABLED | SUBOPT_CREATE_SLOT |
                           SUBOPT_SLOT_NAME | SUBOPT_COPY_DATA | SUBOPT_SYNCHRONOUS_COMMIT |
                           SUBOPT_BINARY | SUBOPT_STREAMING | SUBOPT_TWOPHASE_COMMIT |
                           SUBOPT_DISABLE_ON_ERR | SUBOPT_PASSWORD_REQUIRED |
                           SUBOPT_RUN_AS_OWNER | SUBOPT_FAILOVER | SUBOPT_ORIGIN);
    parse_subscription_options(pstate, stmt->options, supported_opts, &opts);

    // Prevent creation in transaction block if creating replication slot
    if (opts.create_slot)
        PreventInTransactionBlock(isTopLevel,
                                "CREATE SUBSCRIPTION ... WITH (create_slot = true)");

    // Verify privileges - requires pg_create_subscription role
    if (!has_privs_of_role(owner, ROLE_PG_CREATE_SUBSCRIPTION))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to create subscription")));

    // Check CREATE permission on database
    if (object_aclcheck(DatabaseRelationId, MyDatabaseId, owner, ACL_CREATE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_DATABASE, get_database_name(MyDatabaseId));

    // Enforce password requirement for non-superusers
    if (!opts.passwordrequired && !superuser_arg(owner))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("password_required=false is superuser-only")));

    // Open subscription catalog and check for duplicates
    Relation rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    subid = GetSysCacheOid2(SUBSCRIPTIONNAME, Anum_pg_subscription_oid,
                          MyDatabaseId, CStringGetDatum(stmt->subname));
    if (OidIsValid(subid))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("subscription \"%s\" already exists", stmt->subname)));

    // Set default options
    if (!IsSet(opts.specified_opts, SUBOPT_SLOT_NAME) && opts.slot_name == NULL)
        opts.slot_name = stmt->subname;
    if (opts.synchronous_commit == NULL)
        opts.synchronous_commit = "off";

    // Load libpq library and validate connection
    load_file("libpqwalreceiver", false);
    walrcv_check_conninfo(conninfo, opts.passwordrequired && !superuser());

    // Build subscription tuple
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    subid = GetNewOidWithIndex(rel, SubscriptionObjectIndexId, Anum_pg_subscription_oid);
    values[Anum_pg_subscription_oid - 1] = ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_subdbid - 1] = ObjectIdGetDatum(MyDatabaseId);
    values[Anum_pg_subscription_subskiplsn - 1] = LSNGetDatum(InvalidXLogRecPtr);
    values[Anum_pg_subscription_subname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->subname));
    values[Anum_pg_subscription_subowner - 1] = ObjectIdGetDatum(owner);
    values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(opts.enabled);
    values[Anum_pg_subscription_subbinary - 1] = BoolGetDatum(opts.binary);
    values[Anum_pg_subscription_substream - 1] = CharGetDatum(opts.streaming);
    values[Anum_pg_subscription_subtwophasestate - 1] =
        CharGetDatum(opts.twophase ? LOGICALREP_TWOPHASE_STATE_PENDING :
                                   LOGICALREP_TWOPHASE_STATE_DISABLED);
    values[Anum_pg_subscription_subdisableonerr - 1] = BoolGetDatum(opts.disableonerr);
    values[Anum_pg_subscription_subpasswordrequired - 1] = BoolGetDatum(opts.passwordrequired);
    values[Anum_pg_subscription_subrunasowner - 1] = BoolGetDatum(opts.runasowner);
    values[Anum_pg_subscription_subfailover - 1] = BoolGetDatum(opts.failover);
    values[Anum_pg_subscription_subconninfo - 1] = CStringGetTextDatum(conninfo);

    if (opts.slot_name)
        values[Anum_pg_subscription_subslotname - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(opts.slot_name));
    else
        nulls[Anum_pg_subscription_subslotname - 1] = true;

    values[Anum_pg_subscription_subsynccommit - 1] = CStringGetTextDatum(opts.synchronous_commit);
    values[Anum_pg_subscription_subpublications - 1] = publicationListToArray(publications);
    values[Anum_pg_subscription_suborigin - 1] = CStringGetTextDatum(opts.origin);

    // Insert catalog entry
    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Record dependencies and create replication origin
    recordDependencyOnOwner(SubscriptionRelationId, subid, owner);

    char originname[NAMEDATALEN];
    ReplicationOriginNameForLogicalRep(subid, InvalidOid, originname, sizeof(originname));
    replorigin_create(originname);

    // Connect to publisher if requested
    if (opts.connect) {
        bool must_use_password = !superuser_arg(owner) && opts.passwordrequired;
        char *err;
        WalReceiverConn *wrconn = walrcv_connect(conninfo, true, true,
                                               must_use_password, stmt->subname, &err);
        if (!wrconn)
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                           errmsg("could not connect to the publisher: %s", err)));

        PG_TRY();
        {
            // Validate publications and get table list
            check_publications(wrconn, publications);
            check_publications_origin(wrconn, publications, opts.copy_data,
                                    opts.origin, NULL, 0, stmt->subname);

            char table_state = opts.copy_data ? SUBREL_STATE_INIT : SUBREL_STATE_READY;
            List *tables = fetch_table_list(wrconn, publications);

            // Initialize table states
            ListCell *lc;
            foreach(lc, tables) {
                RangeVar *rv = (RangeVar *) lfirst(lc);
                Oid relid = RangeVarGetRelid(rv, AccessShareLock, false);
                CheckSubscriptionRelkind(get_rel_relkind(relid), rv->schemaname, rv->relname);
                AddSubscriptionRelState(subid, relid, table_state, InvalidXLogRecPtr, true);
            }

            // Create replication slot if requested
            if (opts.create_slot) {
                bool twophase_enabled = opts.twophase && !opts.copy_data && tables != NIL;
                walrcv_create_slot(wrconn, opts.slot_name, false, twophase_enabled,
                                 opts.failover, CRS_NOEXPORT_SNAPSHOT, NULL);

                if (twophase_enabled)
                    UpdateTwoPhaseState(subid, LOGICALREP_TWOPHASE_STATE_ENABLED);

                ereport(NOTICE, (errmsg("created replication slot \"%s\" on publisher",
                                      opts.slot_name)));
            }
        }
        PG_FINALLY();
        {
            walrcv_disconnect(wrconn);
        }
        PG_END_TRY();
    }
    else {
        ereport(WARNING, (errmsg("subscription was created, but is not connected")));
    }

    table_close(rel, RowExclusiveLock);

    // Final setup
    pgstat_create_subscription(subid);
    if (opts.enabled)
        ApplyLauncherWakeupAtCommit();

    ObjectAddress myself;
    ObjectAddressSet(myself, SubscriptionRelationId, subid);
    InvokeObjectPostCreateHook(SubscriptionRelationId, subid, 0);

    return myself;
}
```