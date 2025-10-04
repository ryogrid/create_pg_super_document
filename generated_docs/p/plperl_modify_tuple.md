# plperl_modify_tuple

## Location
[src/pl/plperl/plperl.c:1762-1851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1762-L1851)

## Overview
Constructs a modified tuple to be returned from a Perl trigger function by converting Perl hash data back to PostgreSQL heap tuple format.

## Definition

```c
static HeapTuple
plperl_modify_tuple(HV *hvTD, TriggerData *tdata, HeapTuple otup)
```
## Detailed Description
This function takes modifications made by a Perl trigger function (stored in a Perl hash) and applies them to create a new PostgreSQL heap tuple. It extracts the 'new' key from the trigger data hash, validates that it contains a proper hash reference, and then iterates through each key-value pair to construct the modified tuple. The function performs extensive validation including checking for nonexistent columns, system attributes, and generated columns. It serves as a critical bridge for converting Perl-side data modifications back to PostgreSQL's internal tuple representation.

## Parameters / Member Variables
- `*hvTD`: Perl hash containing trigger data, including the 'new' hash with modified column values
- `*tdata`: PostgreSQL trigger data structure containing relation information and tuple descriptors
- `otup`: Original heap tuple that serves as the base for modifications
## Dependencies
- Functions called/Symbols referenced:
  - [hv_fetch_string](../h/hv_fetch_string.md) (fetch value from Perl hash by string key)
  - SvOK, SvROK, SvTYPE, SvRV (Perl API macros for type checking)
  - hv_iterinit, hv_iternext (Perl hash iteration functions)
  - [hek2cstr](../h/hek2cstr.md) (convert Perl hash key to C string)
  - [SPI_fnumber](../S/SPI_fnumber.md) (get attribute number by name)
  - TupleDescAttr (get attribute descriptor)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (convert Perl scalar to PostgreSQL datum)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (PostgreSQL function to create modified tuple)
  - [palloc0](palloc0.md), pfree (PostgreSQL memory management)
- Called from (representative examples):
  - [plperl_trigger_handler](plperl_trigger_handler.md)

## Notes and Other Information
- Validates that ->{new} exists and is a hash reference before processing
- Prevents modification of system attributes (attribute numbers <= 0)
- Prevents modification of generated columns 
- Allocates arrays for modified values, nulls, and replacement flags
- Uses SPI_ERROR_NOATTRIBUTE to detect invalid column names
- Properly manages memory allocation and deallocation for temporary arrays
- Returns a new HeapTuple that replaces the original in trigger processing

## Simplified Source

```c
static HeapTuple plperl_modify_tuple(HV *hvTD, TriggerData *tdata, HeapTuple otup)
{
    SV **svp;
    HV *hvNew;
    HE *he;
    HeapTuple rtup;
    TupleDesc tupdesc;
    int natts;
    Datum *modvalues;
    bool *modnulls;
    bool *modrepls;

    // Get and validate the 'new' hash from trigger data
    svp = hv_fetch_string(hvTD, "new");
    if (!svp)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                       errmsg("$_TD->{new} does not exist")));

    if (!SvOK(*svp) || !SvROK(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVHV)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("$_TD->{new} is not a hash reference")));

    hvNew = (HV *) SvRV(*svp);
    tupdesc = tdata->tg_relation->rd_att;
    natts = tupdesc->natts;

    // Allocate arrays for tuple modification
    modvalues = (Datum *) palloc0(natts * sizeof(Datum));
    modnulls = (bool *) palloc0(natts * sizeof(bool));
    modrepls = (bool *) palloc0(natts * sizeof(bool));

    // Process each key-value pair in the new hash
    hv_iterinit(hvNew);
    while ((he = hv_iternext(hvNew)))
    {
        char *key = hek2cstr(he);
        SV *val = HeVAL(he);
        int attn = SPI_fnumber(tupdesc, key);
        Form_pg_attribute attr = TupleDescAttr(tupdesc, attn - 1);

        // Validate column exists and is modifiable
        if (attn == SPI_ERROR_NOATTRIBUTE)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                           errmsg("Perl hash contains nonexistent column \"%s\"", key)));

        if (attn <= 0)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("cannot set system attribute \"%s\"", key)));

        if (attr->attgenerated)
            ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                           errmsg("cannot set generated column \"%s\"", key)));

        // Convert Perl value to PostgreSQL datum
        modvalues[attn - 1] = plperl_sv_to_datum(val, attr->atttypid, attr->atttypmod,
                                                 NULL, NULL, InvalidOid, &modnulls[attn - 1]);
        modrepls[attn - 1] = true;

        pfree(key);
    }

    // Create the modified tuple
    rtup = heap_modify_tuple(otup, tupdesc, modvalues, modnulls, modrepls);

    // Clean up allocated memory
    pfree(modvalues);
    pfree(modnulls);
    pfree(modrepls);

    return rtup;
}
```