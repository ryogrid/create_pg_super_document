# AttrMissing

## Location
src/include/access/tupdesc_details.h: 22 - 26

## Overview
AttrMissing is a structure that represents default values to be used when an attribute is not present in a tuple, typically occurring when a column was added to a table after the tuple was created.

## Definition

```c
typedef struct AttrMissing
{
	bool		am_present;		/* true if non-NULL missing value exists */
	Datum		am_value;		/* value when attribute is missing */
} AttrMissing;
```
## Detailed Description
The AttrMissing structure is a critical component of PostgreSQL's tuple descriptor system that handles schema evolution scenarios. When a new column is added to an existing table with a default value, existing tuples on disk don't physically contain the new column. Instead of rewriting all existing tuples, PostgreSQL uses the AttrMissing mechanism to provide default values for these "missing" attributes when tuples are read.

This structure is part of PostgreSQL's efficient approach to handling ALTER TABLE ADD COLUMN operations with default values. Rather than immediately updating every row on disk (which would be expensive for large tables), PostgreSQL stores the missing attribute information in the tuple descriptor and applies the default values during tuple reconstruction.

The AttrMissing structure is typically used as part of an array within the TupleConstr structure, with one AttrMissing entry for each attribute that might be missing from older tuples.

## Parameters / Member Variables
- : A boolean flag indicating whether a non-NULL default value exists for this missing attribute. When true, am_value contains a valid default value; when false, the missing attribute should be treated as NULL.
- : The actual default value (as a Datum) to use when the attribute is missing from a tuple. This value is only meaningful when am_present is true.

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL's generic data type)
  - bool (standard boolean type)

- Called from (representative examples):
  -  (src/backend/access/common/heaptuple.c:159)
  -  (src/backend/access/common/heaptuple.c:833)
  -  (src/backend/access/common/tupdesc.c:204-205)
  -  (src/backend/access/common/tupdesc.c:353)
  -  (src/backend/access/common/tupdesc.c:517-518)
  -  (src/backend/executor/execTuples.c:1957)
  -  (src/backend/utils/cache/relcache.c:529, 617, 620)
  -  (src/include/access/tupdesc.h:41)

## Notes and Other Information
- AttrMissing is defined in src/include/access/tupdesc_details.h, indicating it's part of the internal tuple descriptor implementation details
- This mechanism is essential for PostgreSQL's performance when adding columns with default values to large tables
- The structure is closely integrated with the TupleConstr system, where it's stored as an array in the 'missing' field
- When accessing attributes from tuples that predate the addition of certain columns, PostgreSQL uses this structure to seamlessly provide the appropriate default values
- This design allows PostgreSQL to maintain backward compatibility with existing stored tuples while supporting schema evolution
- The am_present field is crucial for distinguishing between a default value of NULL (am_present=false) and an actual default value that happens to be stored as NULL (am_present=true, am_value=NULL Datum)