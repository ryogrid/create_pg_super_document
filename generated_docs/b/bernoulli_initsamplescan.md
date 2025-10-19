# bernoulli_initsamplescan

## Location
[src/backend/access/tablesample/bernoulli.c:127-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/tablesample/bernoulli.c#L127-L135)

## Overview
This function initializes the sample scan state during executor setup by allocating memory for the Bernoulli sampler's private data structure.

## Definition
```c
static void bernoulli_initsamplescan(SampleScanState *node, int eflags)
```

## Detailed Description
The `bernoulli_initsamplescan` function is called during PostgreSQL executor initialization to set up the private state needed for Bernoulli sampling. It allocates and zero-initializes a BernoulliSamplerData structure that will hold the sampling-specific state throughout the scan execution. This is a minimal initialization step that prepares the infrastructure for the actual sampling work that will be performed in subsequent scan phases.

## Parameters / Member Variables
- `node`: SampleScanState structure representing the sample scan execution node
- `eflags`: Executor flags indicating scan behavior options (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - BernoulliSamplerData (private data structure for Bernoulli sampling state)
- Called from (representative examples):
  - [tsm_bernoulli_handler](../t/tsm_bernoulli_handler.md) (sets this as InitSampleScan callback)

## Notes and Other Information
- This is a static function, only callable within the bernoulli.c module
- The function uses palloc0 to ensure the allocated BernoulliSamplerData structure is zero-initialized
- The eflags parameter is accepted for interface compatibility but not currently used
- The allocated memory will be automatically freed when the memory context is destroyed
- This initialization occurs once per scan, before any actual sampling begins
- The BernoulliSamplerData structure will be used to store sampling parameters and state during scan execution

## Simplified Source
```c
static void bernoulli_initsamplescan(SampleScanState *node, int eflags)
{
    // Allocate and initialize Bernoulli sampler state
    node->tsm_state = palloc0(sizeof(BernoulliSamplerData));
}
```