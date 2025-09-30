# transformGenericOptions

## Location
[src/backend/commands/foreigncmds.c:121-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L121-L215)

## Overview
Transforms a list of DefElem structures into text array format while supporting SET/ADD/DROP actions for modifying existing options, with optional validation through a foreign data wrapper validator function.

## Definition
```c
Datum transformGenericOptions(Oid catalogId, Datum oldOptions, List *options, Oid fdwvalidator)
```

## Detailed Description
This function extends the functionality of optionListToArray() by supporting modification operations (SET, ADD, DROP) on existing option lists. It starts with an existing set of options (oldOptions) and applies a series of operations specified in the options list. Each DefElem in the options list can have different actions: DROP removes an existing option, SET modifies an existing option's value, and ADD/UNSPEC adds a new option. The function validates that DROP/SET operations target existing options and that ADD operations don't create duplicates. If a validator function is specified, it's called to validate the final option set. This function is used by CREATE/ALTER commands for foreign data wrappers, servers, user mappings, and foreign tables.

## Parameters / Member Variables
- `catalogId`: OID of the catalog being modified (used by validator function)
- `oldOptions`: Datum representing the existing options array to be modified
- `options`: List of DefElem structures specifying the operations to perform
- `fdwvalidator`: OID of validator function to call on the result, or InvalidOid if no validation needed

## Dependencies
- Functions called/Symbols referenced:
  - [untransformRelOptions](../u/untransformRelOptions.md) (converts Datum array back to DefElem list)
  - [DefElem](../D/DefElem.md) (structure representing option definitions)
  - DEFELEM_DROP, DEFELEM_SET, DEFELEM_ADD, DEFELEM_UNSPEC (action type constants)
  - [list_delete_cell](../l/list_delete_cell.md) (removes cell from linked list)
  - [optionListToArray](../o/optionListToArray.md) (converts final option list back to array format)
  - [construct_empty_array](../c/construct_empty_array.md) (creates empty array for validator)
  - OidFunctionCall2 (calls validator function)
- Called from (representative examples):
  - [CreateForeignDataWrapper](../C/CreateForeignDataWrapper.md) (src/backend/commands/foreigncmds.c:630)
  - [AlterForeignDataWrapper](../A/AlterForeignDataWrapper.md) (src/backend/commands/foreigncmds.c:781)
  - [CreateForeignServer](../C/CreateForeignServer.md) (src/backend/commands/foreigncmds.c:941)
  - [AlterForeignServer](../A/AlterForeignServer.md) (src/backend/commands/foreigncmds.c:1049)
  - [CreateUserMapping](../C/CreateUserMapping.md) (src/backend/commands/foreigncmds.c:1185)
  - [AlterUserMapping](../A/AlterUserMapping.md) (src/backend/commands/foreigncmds.c:1299)
  - [CreateForeignTable](../C/CreateForeignTable.md) (src/backend/commands/foreigncmds.c:1462)
  - [ATExecAlterColumnGenericOptions](../A/ATExecAlterColumnGenericOptions.md) (src/backend/commands/tablecmds.c:14429)
  - [ATExecGenericOptions](../A/ATExecGenericOptions.md) (src/backend/commands/tablecmds.c:16969)

## Notes and Other Information
- Supports multiple SET/DROP actions on the same option as permitted by SQL standards
- Validates that options being added are unique and options being modified/dropped exist
- The validator function receives either the actual options array or an empty array if result is NULL
- This is a key function in PostgreSQL's foreign data wrapper infrastructure for managing configuration options
- Actions default to ADD when not explicitly specified (DEFELEM_UNSPEC)

## Simplified Source

```c
Datum
transformGenericOptions(Oid catalogId, Datum oldOptions, List *options, Oid fdwvalidator)
{
    List *resultOptions = untransformRelOptions(oldOptions);
    ListCell *optcell;
    Datum result;

    // Process each option in the input list
    foreach(optcell, options)
    {
        DefElem *od = lfirst(optcell);
        ListCell *cell;

        // Find existing option with same name in result list
        foreach(cell, resultOptions)
        {
            DefElem *def = lfirst(cell);
            if (strcmp(def->defname, od->defname) == 0)
                break;
        }

        // Apply the requested action to the option
        switch (od->defaction)
        {
            case DEFELEM_DROP:
                if (!cell)
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                    errmsg("option \"%s\" not found", od->defname)));
                resultOptions = list_delete_cell(resultOptions, cell);
                break;

            case DEFELEM_SET:
                if (!cell)
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                    errmsg("option \"%s\" not found", od->defname)));
                lfirst(cell) = od;  // Replace existing option value
                break;

            case DEFELEM_ADD:
            case DEFELEM_UNSPEC:
                if (cell)
                    ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                                    errmsg("option \"%s\" provided more than once", od->defname)));
                resultOptions = lappend(resultOptions, od);
                break;

            default:
                elog(ERROR, "unrecognized action %d on option \"%s\"",
                     (int) od->defaction, od->defname);
        }
    }

    // Convert final option list back to array format
    result = optionListToArray(resultOptions);

    // Call validator function if specified
    if (OidIsValid(fdwvalidator))
    {
        Datum valarg = result;

        // Pass empty array instead of NULL to validator
        if (DatumGetPointer(valarg) == NULL)
            valarg = PointerGetDatum(construct_empty_array(TEXTOID));

        OidFunctionCall2(fdwvalidator, valarg, ObjectIdGetDatum(catalogId));
    }

    return result;
}
```