# transformRelOptions

## Location
[src/backend/access/common/reloptions.c:1156-1339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1156-L1339)

## Overview
Transforms a list of relation option definitions (DefElem) into a text array format suitable for storage in pg_class.reloptions, handling CREATE/ALTER/RESET operations with namespace filtering.

## Definition

```c
struct_array_builtin(array, TEXTOID, &oldoptions, NULL, &noldoptions);
```
## Detailed Description
This function is the core transformation engine for PostgreSQL relation options. It processes a list of option definitions and converts them into the standardized text array format used internally by PostgreSQL. The function handles three main scenarios: CREATE TABLE/INDEX (building from scratch), ALTER TABLE SET (adding/modifying options), and ALTER TABLE RESET (removing options). It performs namespace validation, merges new options with existing ones, and formats each option as 'name=value' strings. The function also includes special handling for deprecated OIDS options and validates that option names don't contain '=' characters.

## Parameters / Member Variables
- : Existing reloptions as Datum (text array format), may be NULL
- : List of DefElem nodes containing new option definitions to process  
- : Target namespace to filter options (NULL means no namespace)
- : Array of valid namespace strings, NULL-terminated (NULL means only NULL namespace valid)
- : Whether to allow 'oids=false' for backwards compatibility
- : True for RESET operations, false for CREATE/SET operations

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - DatumGetArrayTypeP  
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - VARDATA/VARSIZE macros
  - [accumArrayResult](../a/accumArrayResult.md)
  - [makeArrayResult](../m/makeArrayResult.md)
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)
  - [DefElem](../D/DefElem.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (table creation)
  - [DefineIndex](../D/DefineIndex.md) (index creation)
  - [ATExecSetOptions](../A/ATExecSetOptions.md) (ALTER TABLE)
  - [CreateTableSpace](../C/CreateTableSpace.md) (tablespace creation)

## Notes and Other Information
- Returns text array as Datum, or (Datum) 0 if no options
- For RESET operations, validates that no values are specified (syntax checking)
- Handles special case for deprecated OIDS option with backwards compatibility
- Each option is stored as 'name=value' format, with 'name=true' assumed for bare names
- Namespace filtering allows different subsystems to manage their own option sets
- Function is defined in src/backend/access/common/reloptions.c:1156-1339

## Simplified Source
```c
/*
 * Transform a relation options list (list of DefElem) into the text array
 * format that is kept in pg_class.reloptions, including only those options
 * that are in the passed namespace.
 */
Datum
transformRelOptions(Datum oldOptions, List *defList, const char *namspace,
                    char *validnsps[], bool acceptOidsOff, bool isReset)
{
    Datum result;
    ArrayBuildState *astate;
    ListCell *cell;

    /* no change if empty list */
    if (defList == NIL)
        return oldOptions;

    /* We build new array using accumArrayResult */
    astate = NULL;

    /* Copy any oldOptions that aren't to be replaced */
    if (PointerIsValid(DatumGetPointer(oldOptions)))
    {
        ArrayType *array = DatumGetArrayTypeP(oldOptions);
        Datum *oldoptions;
        int noldoptions;
        int i;

        deconstruct_array_builtin(array, TEXTOID, &oldoptions, NULL, &noldoptions);

        for (i = 0; i < noldoptions; i++)
        {
            char *text_str = VARDATA(oldoptions[i]);
            int text_len = VARSIZE(oldoptions[i]) - VARHDRSZ;

            /* Search for a match in defList */
            foreach(cell, defList)
            {
                DefElem *def = (DefElem *) lfirst(cell);
                int kw_len;

                /* ignore if not in the same namespace */
                if (namspace == NULL)
                {
                    if (def->defnamespace != NULL)
                        continue;
                }
                else if (def->defnamespace == NULL)
                    continue;
                else if (strcmp(def->defnamespace, namspace) != 0)
                    continue;

                kw_len = strlen(def->defname);
                if (text_len > kw_len && text_str[kw_len] == '=' &&
                    strncmp(text_str, def->defname, kw_len) == 0)
                    break;
            }
            if (!cell)
            {
                /* No match, so keep old option */
                astate = accumArrayResult(astate, oldoptions[i],
                                          false, TEXTOID,
                                          CurrentMemoryContext);
            }
        }
    }

    /*
     * If CREATE/SET, add new options to array; if RESET, just check that the
     * user didn't say RESET (option=val).
     */
    foreach(cell, defList)
    {
        DefElem *def = (DefElem *) lfirst(cell);

        if (isReset)
        {
            if (def->arg != NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("RESET must not include values for parameters")));
        }
        else
        {
            const char *name;
            const char *value;
            text *t;
            Size len;

            /* Error out if the namespace is not valid */
            if (def->defnamespace != NULL)
            {
                bool valid = false;
                int i;

                if (validnsps)
                {
                    for (i = 0; validnsps[i]; i++)
                    {
                        if (strcmp(def->defnamespace, validnsps[i]) == 0)
                        {
                            valid = true;
                            break;
                        }
                    }
                }

                if (!valid)
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("unrecognized parameter namespace \"%s\"",
                                    def->defnamespace)));
            }

            /* ignore if not in the same namespace */
            if (namspace == NULL)
            {
                if (def->defnamespace != NULL)
                    continue;
            }
            else if (def->defnamespace == NULL)
                continue;
            else if (strcmp(def->defnamespace, namspace) != 0)
                continue;

            /* Flatten the DefElem into a text string like "name=arg" */
            name = def->defname;
            if (def->arg != NULL)
                value = defGetString(def);
            else
                value = "true";

            /* Validate option name doesn't contain "=" */
            if (strchr(name, '=') != NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid option name \"%s\": must not contain \"=\"",
                                name)));

            /* Handle deprecated OIDS option */
            if (acceptOidsOff && def->defnamespace == NULL &&
                strcmp(name, "oids") == 0)
            {
                if (defGetBoolean(def))
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("tables declared WITH OIDS are not supported")));
                /* skip over option, reloptions machinery doesn't know it */
                continue;
            }

            len = VARHDRSZ + strlen(name) + 1 + strlen(value);
            /* +1 leaves room for sprintf's trailing null */
            t = (text *) palloc(len + 1);
            SET_VARSIZE(t, len);
            sprintf(VARDATA(t), "%s=%s", name, value);

            astate = accumArrayResult(astate, PointerGetDatum(t),
                                      false, TEXTOID,
                                      CurrentMemoryContext);
        }
    }

    if (astate)
        result = makeArrayResult(astate, CurrentMemoryContext);
    else
        result = (Datum) 0;

    return result;
}
```