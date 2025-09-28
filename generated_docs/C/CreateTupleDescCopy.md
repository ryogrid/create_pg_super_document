# CreateTupleDescCopy

## Location
[src/backend/access/common/tupdesc.c:133-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L133-L172)

## Overview
Creates a new tuple descriptor by copying the basic structure and attributes from an existing TupleDesc, but explicitly excludes constraints and defaults.

## Definition
```c
TupleDesc CreateTupleDescCopy(TupleDesc tupdesc)
```

## Detailed Description
This function creates a shallow copy of a tuple descriptor, copying the basic attribute information while deliberately excluding constraints, defaults, and other advanced features. It first creates a template using CreateTemplateTupleDesc, then performs a flat copy of the entire attribute array. However, it then explicitly clears constraint-related fields (attnotnull, atthasdef, atthasmissing, attidentity, attgenerated) for each attribute to ensure the copy has no constraints or defaults. The tuple type identification (tdtypeid and tdtypmod) is preserved from the original. This function is commonly used when you need a basic structural copy of a tuple descriptor without its behavioral constraints.

## Parameters / Member Variables
- `tupdesc`: The source tuple descriptor to copy from

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](CreateTemplateTupleDesc.md)
  - memcpy
  - TupleDescAttr (macro for accessing attributes)
  - FormData_pg_attribute, Form_pg_attribute (types)
- Called from (representative examples):
  - [getSpGistTupleDesc](../g/getSpGistTupleDesc.md)
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md)
  - [ExecEvalWholeRowVar](../E/ExecEvalWholeRowVar.md)
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md)
  - [ExecPrepareTuplestoreResult](../E/ExecPrepareTuplestoreResult.md)
  - [RelationBuildLocalRelation](../R/RelationBuildLocalRelation.md)
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md)

## Notes and Other Information
- **CRITICAL**: Constraints and defaults are explicitly NOT copied and are cleared
- Performs a flat copy of the attribute array using memcpy for efficiency
- Preserves tuple type identification (tdtypeid, tdtypmod) from the source
- Clears constraint-related fields: attnotnull, atthasdef, atthasmissing, attidentity, attgenerated
- Commonly used in execution contexts where structural compatibility is needed but constraints should not be enforced
- More efficient than creating attributes individually when you have a source descriptor to copy from
- The resulting descriptor has the same number and types of attributes but with simplified metadata
- Used extensively in query execution, SPI operations, and temporary table scenarios
- Does not copy the constr (constraints) structure from the original descriptor

## Simplified Source

```c
// Simplified version of CreateTupleDescCopy
TupleDesc CreateTupleDescCopy(TupleDesc tupdesc) {
    // Create template with same number of attributes
    TupleDesc desc = CreateTemplateTupleDesc(tupdesc->natts);

    // Copy all attribute data in one operation
    memcpy(TupleDescAttr(desc, 0), TupleDescAttr(tupdesc, 0),
           desc->natts * sizeof(FormData_pg_attribute));

    // Clear constraint-related fields for all attributes
    for (int i = 0; i < desc->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(desc, i);

        att->attnotnull = false;
        att->atthasdef = false;
        att->atthasmissing = false;
        att->attidentity = '\0';
        att->attgenerated = '\0';
    }

    // Preserve type identification from original
    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;

    return desc;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Focused on the core copying and clearing operations
- Maintained the essential flat copy + constraint clearing pattern
- Preserved type identification copying