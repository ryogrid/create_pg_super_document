# createPQExpBuffer

## Location
src/interfaces/libpq/pqexpbuffer.c: 72 - 89

## Overview
Creates and returns a new PQExpBuffer by allocating memory for both the PQExpBufferData structure and initializing it with an empty buffer.

## Definition

```c
struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initPQExpBuffer(PQExpBuffer str)
{
	str->data = (char *) malloc(INITIAL_EXPBUFFER_SIZE);
	if (str->data == NULL)
	{
		str->data = unconstify(char *, oom_buffer_ptr); /* see comment above */
		str->maxlen = 0;
		str->len = 0;
	}
	else
	{
		str->maxlen = INITIAL_EXPBUFFER_SIZE;
		str->len = 0;
		str->data[0] = '\0';
	}
}

/*
 * destroyPQExpBuffer(str);
```
## Detailed Description
This function provides a convenient way to create a new PQExpBuffer object with both the structure and its data buffer dynamically allocated. It serves as a high-level constructor that:

1. Allocates memory for a PQExpBufferData structure using 
2. If allocation succeeds, initializes the structure by calling 
3. Returns the pointer to the new buffer, or NULL if memory allocation failed

This is one of two standard ways to create a PQExpBuffer (the other being  for pre-allocated structures). The function handles the common case where both the structure and its data need to be dynamically allocated.

## Parameters / Member Variables
- None (void function)

## Return Value
- Returns a pointer to a newly allocated and initialized PQExpBuffer
- Returns NULL if memory allocation for the structure fails

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function for memory allocation)
  -  (initializes the allocated PQExpBufferData structure)
  -  (the structure type being allocated)

- Called from:
  - External libpq client applications (no internal references found in current analysis)

## Notes and Other Information
- This function allocates memory that must later be freed using 
- If  fails, the function returns NULL without calling 
- The caller should check the return value for NULL before using the buffer
- This function is part of the public libpq API for client applications
- Prefer this function when you need a completely dynamic PQExpBuffer; use  when the PQExpBufferData structure is already allocated (e.g., as a struct member)