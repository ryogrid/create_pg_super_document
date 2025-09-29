# intorel_startup

## Location
[src/backend/commands/createas.c:452-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L452-L575)

## Overview
intorel_startup initializes the destination receiver for CREATE TABLE AS and CREATE MATERIALIZED VIEW operations by creating the target relation, setting up column definitions, and preparing the state for bulk tuple insertion.

## Definition
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)

## Detailed Description
This function serves as the startup callback for DR_intorel destination receivers. It performs the complete setup process for creating a new relation from query results. The function builds column definitions from the provided tuple descriptor, optionally overriding column names from the IntoClause specification. It then creates the actual target table using create_ctas_internal(), opens it with exclusive access, validates RLS policies, and initializes the state needed for efficient bulk insertion of tuples. For materialized views that will be populated, it tentatively marks them as populated.

## Parameters / Member Variables
- : The DestReceiver object cast to DR_intorel containing the target specification
- : The executor operation type (unused in this function)
- : TupleDesc describing the structure and types of tuples to be inserted

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md)
  - [lnext](../l/lnext.md)
  - [makeColumnDef](../m/makeColumnDef.md)
  - [type_is_collatable](../t/type_is_collatable.md)
  - [create_ctas_internal](../c/create_ctas_internal.md)
  - [table_open](../t/table_open.md)
  - [check_enable_rls](../c/check_enable_rls.md)
  - [SetMatViewPopulatedState](../S/SetMatViewPopulatedState.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [GetBulkInsertState](../G/GetBulkInsertState.md)
  - RelationGetTargetBlock
- Called from (representative examples):
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md) (sets as callback)
  - DR_intorel structure initialization

## Notes and Other Information
The function supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW by checking the viewQuery field of the IntoClause. It validates that collations can be resolved for collatable types and ensures RLS policies are not enabled (not yet supported for these operations). The function sets up bulk insertion state only when data will actually be inserted (skipData is false). An assertion ensures the target relation's block number is invalid, indicating no prior writes to the relation.

## Simplified Source

```c
// Simplified version of intorel_startup
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo) {
    DR_intorel *myState = (DR_intorel *) self;
    IntoClause *into = myState->into;
    bool is_matview = (into->viewQuery != NULL);
    List *attrList = NIL;
    ObjectAddress intoRelationAddr;
    Relation intoRelationDesc;

    // Build column definitions from tuple descriptor
    ListCell *lc = list_head(into->colNames);
    for (int attnum = 0; attnum < typeinfo->natts; attnum++) {
        Form_pg_attribute attribute = TupleDescAttr(typeinfo, attnum);
        char *colname;

        // Use provided column name or default from attribute
        if (lc) {
            colname = strVal(lfirst(lc));
            lc = lnext(into->colNames, lc);
        } else {
            colname = NameStr(attribute->attname);
        }

        // Create column definition
        ColumnDef *col = makeColumnDef(colname,
                                     attribute->atttypid,
                                     attribute->atttypmod,
                                     attribute->attcollation);

        // Validate collation for collatable types
        if (!OidIsValid(col->collOid) && type_is_collatable(col->typeName->typeOid)) {
            ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_COLLATION),
                          errmsg("no collation derived for column \"%s\"", col->colname)));
        }

        attrList = lappend(attrList, col);
    }

    // Check for excess column names
    if (lc != NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("too many column names were specified")));
    }

    // Create the target table
    intoRelationAddr = create_ctas_internal(attrList, into);

    // Open the target table with exclusive lock
    intoRelationDesc = table_open(intoRelationAddr.objectId, AccessExclusiveLock);

    // Ensure RLS is not enabled (not supported)
    if (check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("policies not yet implemented for this command")));
    }

    // Mark materialized view as populated if data will be inserted
    if (is_matview && !into->skipData) {
        SetMatViewPopulatedState(intoRelationDesc, true);
    }

    // Initialize state for bulk insertion
    myState->rel = intoRelationDesc;
    myState->reladdr = intoRelationAddr;
    myState->output_cid = GetCurrentCommandId(true);
    myState->ti_options = TABLE_INSERT_SKIP_FSM;

    // Set up bulk insert state only if data will be inserted
    if (!into->skipData) {
        myState->bistate = GetBulkInsertState();
    } else {
        myState->bistate = NULL;
    }
}
```

Key simplifications made:
- Removed detailed error message formatting for brevity
- Consolidated variable declarations
- Simplified comments to focus on main logic flow
- Removed assertion and detailed RLS error message content
- Streamlined the column building loop structure
- Abstracted away complex error reporting details while preserving error conditions