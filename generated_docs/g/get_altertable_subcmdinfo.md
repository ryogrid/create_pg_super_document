# get_altertable_subcmdinfo

## Location
[src/test/modules/test_ddl_deparse/test_ddl_deparse.c:87-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_ddl_deparse/test_ddl_deparse.c#L87-L335)

## Overview
Returns a text array representation of the subcommands contained within an ALTER TABLE command, providing detailed information about each subcommand type and associated objects.

## Definition
```c
Datum get_altertable_subcmdinfo(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the test_ddl_deparse module and serves as a comprehensive utility for analyzing ALTER TABLE commands during DDL deparsing tests. It takes a CollectedCommand pointer that must represent an ALTER TABLE command and returns a set of rows (as a materialized set-returning function) containing information about each subcommand within the ALTER TABLE statement.

The function performs extensive validation, ensuring the input command is indeed an ALTER TABLE command, and then iterates through all subcommands, mapping each ALTER TABLE subcommand type enum to its human-readable string representation. For each subcommand, it provides two pieces of information: the subcommand type string (with optional recursion indication) and a description of the target object if available.

The function handles over 40 different ALTER TABLE subcommand types, covering column operations, constraint management, trigger/rule operations, inheritance, partitioning, security settings, and more. This comprehensive coverage makes it a valuable tool for testing and debugging DDL parsing functionality.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract the CollectedCommand pointer)
  - elog (for error reporting)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (to initialize set-returning function)
  - castNode (to cast parse tree nodes)
  - [psprintf](../p/psprintf.md) (for string formatting)
  - CStringGetTextDatum (to convert C strings to PostgreSQL text datums)
  - OidIsValid (to validate object IDs)
  - [getObjectDescription](getObjectDescription.md) (to generate object descriptions)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (to store result tuples)
- Enum values referenced (ALTER TABLE subcommand types):
  - AT_AddColumn, AT_DropColumn, AT_AlterColumnType
  - AT_AddConstraint, AT_DropConstraint, AT_ValidateConstraint
  - AT_EnableTrig, AT_DisableTrig (and variations)
  - AT_AttachPartition, AT_DetachPartition
  - AT_AddIdentity, AT_SetIdentity, AT_DropIdentity
  - Many others covering all ALTER TABLE operations
- Data structures referenced:
  - [CollectedCommand](../C/CollectedCommand.md), CollectedATSubcmd, AlterTableCmd
  - [ReturnSetInfo](../R/ReturnSetInfo.md), ObjectAddress
- Called from:
  - No direct callers found (likely used as a SQL-callable function in tests)

## Notes and Other Information
- This function is specifically designed for testing DDL deparsing functionality
- Requires the input to be an ALTER TABLE command; throws an error for other command types
- Returns detailed information in a two-column format: subcommand type and object description
- Handles recursion by appending "(and recurse)" to the subcommand type string
- Provides NULL for object description when no valid object ID is available
- Comprehensive coverage of all ALTER TABLE subcommand types makes it valuable for thorough testing
- Part of the test_ddl_deparse extension module infrastructure
- The extensive switch statement covers the complete spectrum of ALTER TABLE operations in PostgreSQL

## Simplified Source

```c
Datum get_altertable_subcmdinfo(PG_FUNCTION_ARGS)
{
    CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);
    ListCell *cell;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    // Validate input is ALTER TABLE command
    if (cmd->type != SCT_AlterTable)
        elog(ERROR, "command is not ALTER TABLE");

    InitMaterializedSRF(fcinfo, 0);

    if (cmd->d.alterTable.subcmds == NIL)
        elog(ERROR, "empty alter table subcommand list");

    // Process each subcommand
    foreach(cell, cmd->d.alterTable.subcmds)
    {
        CollectedATSubcmd *sub = lfirst(cell);
        AlterTableCmd *subcmd = castNode(AlterTableCmd, sub->parsetree);
        const char *strtype = "unrecognized";
        Datum values[2];
        bool nulls[2];

        memset(values, 0, sizeof(values));
        memset(nulls, 0, sizeof(nulls));

        // Map subcommand type to string (extensive switch statement)
        switch (subcmd->subtype)
        {
            case AT_AddColumn:
                strtype = "ADD COLUMN";
                break;
            case AT_DropColumn:
                strtype = "DROP COLUMN";
                break;
            case AT_AddConstraint:
                strtype = "ADD CONSTRAINT";
                break;
            case AT_DropConstraint:
                strtype = "DROP CONSTRAINT";
                break;
            // ... many more cases for all ALTER TABLE operations
            default:
                strtype = "unrecognized";
                break;
        }

        // Format result: add recursion info if needed
        if (subcmd->recurse)
            values[0] = CStringGetTextDatum(psprintf("%s (and recurse)", strtype));
        else
            values[0] = CStringGetTextDatum(strtype);

        // Add object description if available
        if (OidIsValid(sub->address.objectId))
        {
            char *objdesc = getObjectDescription((const ObjectAddress *) &sub->address, false);
            values[1] = CStringGetTextDatum(objdesc);
        }
        else
            nulls[1] = true;

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    return (Datum) 0;
}
```