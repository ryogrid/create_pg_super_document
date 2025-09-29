# CreateTupleDescCopyConstr

## Location
[src/backend/access/common/tupdesc.c:173-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L173-L250)

## Overview
Creates a new TupleDesc by deep copying from an existing TupleDesc, including all constraints, defaults, and missing value specifications.

## Definition

```c
structure, if any */
	if (constr)
	{
		TupleConstr *cpy = (TupleConstr *) palloc0(sizeof(TupleConstr));

		cpy->has_not_null = constr->has_not_null;
		cpy->has_generated_stored = constr->has_generated_stored;

		if ((cpy->num_defval = constr->num_defval) > 0)
		{
			cpy->defval = (AttrDefault *) palloc(cpy->num_defval * sizeof(AttrDefault));
			memcpy(cpy->defval, constr->defval, cpy->num_defval * sizeof(AttrDefault));
			for (i = cpy->num_defval - 1; i >= 0; i--)
				cpy->defval[i].adbin = pstrdup(constr->defval[i].adbin);
		}

		if (constr->missing)
		{
			cpy->missing = (AttrMissing *) palloc(tupdesc->natts * sizeof(AttrMissing));
			memcpy(cpy->missing, constr->missing, tupdesc->natts * sizeof(AttrMissing));
			for (i = tupdesc->natts - 1; i >= 0; i--)
			{
				if (constr->missing[i].am_present)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

					cpy->missing[i].am_value = datumCopy(constr->missing[i].am_value,
														 attr->attbyval,
														 attr->attlen);
				}
			}
		}

		if ((cpy->num_check = constr->num_check) > 0)
		{
			cpy->check = (ConstrCheck *) palloc(cpy->num_check * sizeof(ConstrCheck));
			memcpy(cpy->check, constr->check, cpy->num_check * sizeof(ConstrCheck));
			for (i = cpy->num_check - 1; i >= 0; i--)
			{
				cpy->check[i].ccname = pstrdup(constr->check[i].ccname);
				cpy->check[i].ccbin = pstrdup(constr->check[i].ccbin);
				cpy->check[i].ccvalid = constr->check[i].ccvalid;
				cpy->check[i].ccnoinherit = constr->check[i].ccnoinherit;
			}
		}

		desc->constr = cpy;
	}

	/* We can copy the tuple type identification, too */
	desc->tdtypeid = tupdesc->tdtypeid;
```
## Detailed Description
This function performs a complete deep copy of a tuple descriptor, including all associated constraint information. Unlike a simple copy, it duplicates all constraint structures (default values, check constraints, missing values) and their associated data. The function first creates a template tuple descriptor with the same number of attributes, then flat-copies the attribute array, and finally deep-copies all constraint-related data structures to ensure complete independence between the original and copied descriptors.

## Parameters
- `tupdesc`: The source TupleDesc to copy from, including all its constraints and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](CreateTemplateTupleDesc.md)
  - [TupleConstr](../T/TupleConstr.md)
  - [AttrDefault](../A/AttrDefault.md)
  - [AttrMissing](../A/AttrMissing.md)
  - [ConstrCheck](ConstrCheck.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [initGISTstate](../i/initGISTstate.md)
  - [ATGetQueueEntry](../A/ATGetQueueEntry.md)
  - [init_tuple_slot](../i/init_tuple_slot.md)
  - [CatalogCacheInitializeCache](CatalogCacheInitializeCache.md)
  - [lookup_rowtype_tupdesc_copy](../l/lookup_rowtype_tupdesc_copy.md)

## Notes and Other Information
- Performs deep copying of all constraint structures including default values, check constraints, and missing values
- Uses palloc0 and palloc for memory allocation of constraint structures
- Copies tuple type identification (tdtypeid and tdtypmod) from source
- Handles NULL constraint pointers gracefully
- Ensures complete independence between source and destination tuple descriptors

## Simplified Source

```c
TupleDesc CreateTupleDescCopyConstr(TupleDesc tupdesc) {
    TupleConstr *constr = tupdesc->constr;

    // Create template with same number of attributes
    TupleDesc desc = CreateTemplateTupleDesc(tupdesc->natts);

    // Copy attribute array
    memcpy(TupleDescAttr(desc, 0), TupleDescAttr(tupdesc, 0),
           desc->natts * sizeof(FormData_pg_attribute));

    // Copy constraint structure if it exists
    if (constr) {
        TupleConstr *cpy = (TupleConstr *) palloc0(sizeof(TupleConstr));

        // Copy basic constraint flags
        cpy->has_not_null = constr->has_not_null;
        cpy->has_generated_stored = constr->has_generated_stored;

        // Copy default values
        if ((cpy->num_defval = constr->num_defval) > 0) {
            cpy->defval = (AttrDefault *) palloc(cpy->num_defval * sizeof(AttrDefault));
            memcpy(cpy->defval, constr->defval, cpy->num_defval * sizeof(AttrDefault));
            // Deep copy default value expressions
            for (int i = cpy->num_defval - 1; i >= 0; i--) {
                cpy->defval[i].adbin = pstrdup(constr->defval[i].adbin);
            }
        }

        // Copy missing values
        if (constr->missing) {
            cpy->missing = (AttrMissing *) palloc(tupdesc->natts * sizeof(AttrMissing));
            memcpy(cpy->missing, constr->missing, tupdesc->natts * sizeof(AttrMissing));
            // Deep copy missing value datums
            for (int i = tupdesc->natts - 1; i >= 0; i--) {
                if (constr->missing[i].am_present) {
                    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                    cpy->missing[i].am_value = datumCopy(constr->missing[i].am_value,
                                                        attr->attbyval, attr->attlen);
                }
            }
        }

        // Copy check constraints
        if ((cpy->num_check = constr->num_check) > 0) {
            cpy->check = (ConstrCheck *) palloc(cpy->num_check * sizeof(ConstrCheck));
            memcpy(cpy->check, constr->check, cpy->num_check * sizeof(ConstrCheck));
            // Deep copy constraint names and expressions
            for (int i = cpy->num_check - 1; i >= 0; i--) {
                cpy->check[i].ccname = pstrdup(constr->check[i].ccname);
                cpy->check[i].ccbin = pstrdup(constr->check[i].ccbin);
                cpy->check[i].ccvalid = constr->check[i].ccvalid;
                cpy->check[i].ccnoinherit = constr->check[i].ccnoinherit;
            }
        }

        desc->constr = cpy;
    }

    // Copy tuple type identification
    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;

    return desc;
}
```