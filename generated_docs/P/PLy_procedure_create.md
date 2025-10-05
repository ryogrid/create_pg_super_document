# PLy_procedure_create

## Location
[src/pl/plpython/plpy_procedure.c:133-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_procedure.c#L133-L351)

## Overview
Creates a new PLyProcedure structure by parsing function metadata, setting up input/output conversion functions, and compiling the Python source code into a callable procedure object.

## Definition
```c
static PLyProcedure *PLy_procedure_create(HeapTuple procTup, Oid fn_oid, bool is_trigger)
```

## Detailed Description
This comprehensive function constructs a complete PLyProcedure object from PostgreSQL system catalog information. It generates a unique Python function name, creates a dedicated memory context, extracts function metadata (name, arguments, return type), validates type compatibility, sets up input/output conversion functions for all parameters and return values, retrieves the function source code, and finally compiles it using PLy_procedure_compile. The function handles both regular functions and trigger functions with appropriate type checking and setup.

## Parameters / Member Variables
- `procTup`: HeapTuple containing the pg_proc system catalog entry for the function
- `fn_oid`: OID of the function being created
- `is_trigger`: Boolean flag indicating whether this is a trigger function

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)/MemoryContextSetIdentifier (memory management)
  - [SearchSysCache1](../S/SearchSysCache1.md)/SysCacheGetAttr/ReleaseSysCache (system catalog access)
  - [get_func_arg_info](../g/get_func_arg_info.md) (argument information extraction)
  - [PLy_output_setup_func](PLy_output_setup_func.md)/PLy_input_setup_func (I/O function setup)
  - TextDatumGetCString (source code extraction)
  - [PLy_procedure_compile](PLy_procedure_compile.md) (Python compilation)
  - [PLy_procedure_delete](PLy_procedure_delete.md) (cleanup on error)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling)
- Called from (representative examples):
  - [PLy_procedure_get](PLy_procedure_get.md) (cache miss or validation failure scenarios)

## Notes and Other Information
- Creates a dedicated memory context for each procedure to ensure proper cleanup
- Generates Python-safe procedure names by replacing invalid characters with underscores
- Validates argument and return types, rejecting most pseudotypes except void, record, and trigger types
- Handles both regular functions and trigger functions with different setup requirements
- Sets up complete type conversion infrastructure for all input parameters and return values
- Uses exception-safe patterns to ensure cleanup on compilation errors
- The procedure name format is '__plpython_procedure_[original_name]_[oid]' for uniqueness
- Critical for transforming PostgreSQL function definitions into executable Python procedures

## Simplified Source

```c
static PLyProcedure *PLy_procedure_create(HeapTuple procTup, Oid fn_oid, bool is_trigger) {
    char procName[NAMEDATALEN + 256];
    Form_pg_proc procStruct;
    PLyProcedure *proc;
    MemoryContext cxt, oldcxt;
    char *ptr;

    procStruct = (Form_pg_proc) GETSTRUCT(procTup);

    // Generate unique Python function name
    snprintf(procName, sizeof(procName), "__plpython_procedure_%s_%u",
             NameStr(procStruct->proname), fn_oid);

    // Replace non-Python-safe characters with underscores
    for (ptr = procName; *ptr; ptr++) {
        if (!((*ptr >= 'A' && *ptr <= 'Z') ||
              (*ptr >= 'a' && *ptr <= 'z') ||
              (*ptr >= '0' && *ptr <= '9')))
            *ptr = '_';
    }

    // Create dedicated memory context for this procedure
    cxt = AllocSetContextCreate(TopMemoryContext, "PL/Python function",
                                ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(cxt);

    proc = (PLyProcedure *) palloc0(sizeof(PLyProcedure));
    proc->mcxt = cxt;

    PG_TRY(); {
        // Set basic procedure properties
        proc->proname = pstrdup(NameStr(procStruct->proname));
        proc->pyname = pstrdup(procName);
        proc->fn_xmin = HeapTupleHeaderGetRawXmin(procTup->t_data);
        proc->fn_tid = procTup->t_self;
        proc->is_trigger = is_trigger;
        proc->is_setof = procStruct->proretset;

        // Setup return type conversion for non-trigger functions
        if (!is_trigger) {
            Oid rettype = procStruct->prorettype;
            HeapTuple rvTypeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(rettype));

            // Validate return type (reject most pseudotypes)
            Form_pg_type rvTypeStruct = (Form_pg_type) GETSTRUCT(rvTypeTup);
            if (rvTypeStruct->typtype == TYPTYPE_PSEUDO) {
                if (!(rettype == VOIDOID || rettype == RECORDOID))
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("PL/Python functions cannot return type %s",
                                   format_type_be(rettype))));
            }

            PLy_output_setup_func(&proc->result, proc->mcxt, rettype, -1, proc);
            ReleaseSysCache(rvTypeTup);
        }

        // Setup input argument conversion
        if (procStruct->pronargs) {
            Oid *types;
            char **names, *modes;
            int total = get_func_arg_info(procTup, &types, &names, &modes);

            // Count input arguments (exclude OUT parameters)
            proc->nargs = 0;
            for (int i = 0; i < total; i++) {
                if (!modes || (modes[i] != PROARGMODE_OUT && modes[i] != PROARGMODE_TABLE))
                    (proc->nargs)++;
            }

            // Setup conversion for each input argument
            proc->argnames = (char **) palloc0(sizeof(char *) * proc->nargs);
            proc->args = (PLyDatumToOb *) palloc0(sizeof(PLyDatumToOb) * proc->nargs);

            for (int i = 0, pos = 0; i < total; i++) {
                if (modes && (modes[i] == PROARGMODE_OUT || modes[i] == PROARGMODE_TABLE))
                    continue; // Skip OUT arguments

                // Validate argument type
                HeapTuple argTypeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(types[i]));
                Form_pg_type argTypeStruct = (Form_pg_type) GETSTRUCT(argTypeTup);

                if (argTypeStruct->typtype == TYPTYPE_PSEUDO)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("PL/Python functions cannot accept type %s",
                                   format_type_be(types[i]))));

                PLy_input_setup_func(&proc->args[pos], proc->mcxt, types[i], -1, proc);
                proc->argnames[pos] = names ? pstrdup(names[i]) : NULL;

                ReleaseSysCache(argTypeTup);
                pos++;
            }
        }

        // Get function source and compile it
        Datum prosrcdatum = SysCacheGetAttrNotNull(PROCOID, procTup, Anum_pg_proc_prosrc);
        char *procSource = TextDatumGetCString(prosrcdatum);

        PLy_procedure_compile(proc, procSource);
        pfree(procSource);
    }
    PG_CATCH(); {
        MemoryContextSwitchTo(oldcxt);
        PLy_procedure_delete(proc);
        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcxt);
    return proc;
}
```