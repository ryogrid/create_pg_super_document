# JsonbIterState

## Location
src/include/utils/jsonb.h: 339 - 340

## Overview
JsonbIterState is an enumeration that defines the current state of iteration through a JSONB structure, tracking whether the iterator is processing array elements, object keys, object values, or container boundaries.

## Definition

```c
typedef struct JsonbIterator
{
	/* Container being iterated */
	JsonbContainer *container;
	uint32		nElems;			/* Number of elements in children array (will
								 * be nPairs for objects) */
	bool		isScalar;		/* Pseudo-array scalar value? */
	JEntry	   *children;		/* JEntrys for child nodes */
	/* Data proper.  This points to the beginning of the variable-length data */
	char	   *dataProper;

	/* Current item in buffer (up to nElems) */
	int			curIndex;

	/* Data offset corresponding to current item */
	uint32		curDataOffset;

	/*
	 * If the container is an object, we want to return keys and values
	 * alternately; so curDataOffset points to the current key, and
	 * curValueOffset points to the current value.
	 */
	uint32		curValueOffset;

	/* Private state */
	JsonbIterState state;

	struct JsonbIterator *parent;
} JsonbIterator;
```
## Detailed Description
JsonbIterState is a crucial component of PostgreSQL's JSONB iterator mechanism, providing state management for traversing complex JSON structures. This enumeration tracks the current position and context within nested JSON arrays and objects during iteration operations.

The enumeration enables the JsonbIterator to maintain proper context when processing hierarchical JSON data structures. It ensures that the iterator correctly handles the alternating pattern of keys and values in JSON objects, as well as the sequential processing of array elements. The state information is essential for proper JSON output formatting and for implementing JSON path operations.

Each state represents a specific phase in the iteration process:
- Container start states indicate the beginning of processing a new JSON structure
- Element/key/value states indicate processing of actual data content
- The state transitions follow JSON structure rules (objects alternate between keys and values, arrays process elements sequentially)

## Parameters / Member Variables
- : Indicates the iterator is at the beginning of a JSON array
- : Indicates the iterator is processing an element within a JSON array
- : Indicates the iterator is at the beginning of a JSON object
- : Indicates the iterator is processing a key within a JSON object
- : Indicates the iterator is processing a value within a JSON object

## Dependencies
- Functions called/Symbols referenced:
  - Used as member type in JsonbIterator struct
- Called from (representative examples):
  - JsonbIterator (src/include/utils/jsonb.h:366) - as the state field

## Notes and Other Information
- The state transitions ensure proper JSON syntax is maintained during iteration and output generation
- Object iteration alternates between JBI_OBJECT_KEY and JBI_OBJECT_VALUE states to handle key-value pairs correctly
- Array iteration uses JBI_ARRAY_ELEM state for each element after the initial JBI_ARRAY_START
- The state information is critical for implementing JSON path expressions and nested JSON traversal
- This enumeration is part of PostgreSQL's internal JSONB implementation and is not exposed to SQL users directly