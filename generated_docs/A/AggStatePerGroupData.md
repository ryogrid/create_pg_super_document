# AggStatePerGroupData

## Location
src/include/executor/nodeAgg.h: 250 - 267

## Overview
AggStatePerGroupData represents per-aggregate-per-group working state that tracks the current transition value and its status for each group in aggregate processing.

## Definition

```c
typedef struct AggStatePerGroupData
{
#define FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUE 0
	Datum		transValue;		/* current transition value */
#define FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUEISNULL 1
	bool		transValueIsNull;

#define FIELDNO_AGGSTATEPERGROUPDATA_NOTRANSVALUE 2
	bool		noTransValue;	/* true if transValue not set yet */

	/*
	 * Note: noTransValue initially has the same value as transValueIsNull,
	 * and if true both are cleared to false at the same time.  They are not
	 * the same though: if transfn later returns a NULL, we want to keep that
	 * NULL and not auto-replace it with a later input value. Only the first
	 * non-NULL input will be auto-substituted.
	 */
}			AggStatePerGroupData;
```
## Detailed Description
AggStatePerGroupData stores the working state for each group in aggregate processing, containing the current transition value and metadata about its status. This structure is used differently depending on the aggregation mode:

- **AGG_PLAIN and AGG_SORTED modes**: A single array of these structs is maintained (pointed to by aggstate->pergroup), with array reuse for each input group in AGG_SORTED mode.
- **AGG_HASHED mode**: The hash table contains an array of these structs for each tuple group, enabling efficient per-group state management.

The structure handles the distinction between NULL transition values and uninitialized values, which is crucial for correct aggregate behavior, especially for aggregates that should substitute the first non-NULL input value.

## Parameters / Member Variables
- : Current transition value for the aggregate computation (Datum type for type flexibility)
- : Boolean indicating whether the current transition value is NULL
- : Boolean indicating whether the transition value has been set yet (initially true)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - build_hash_table
  - hash_agg_entry_size
  - initialize_hash_entry
  - ExecInitAgg
  - ExecReScanAgg
  - AggStatePerGroup

## Notes and Other Information
The noTransValue and transValueIsNull fields serve different purposes: noTransValue tracks whether any value has been processed yet, while transValueIsNull indicates the current value's NULL status. Initially both have the same value, but they diverge after the first input - noTransValue becomes false permanently, while transValueIsNull reflects the actual NULL status of subsequent transition function results. This distinction ensures that only the first non-NULL input triggers auto-substitution behavior in aggregates that support it.

The structure excludes the sortstate field for space efficiency since DISTINCT aggregates are not supported in AGG_HASHED mode, avoiding unnecessary pointer storage in hash table entries.