# get_nearest_unprocessed_vertex

## Location
src/backend/commands/extension.c: 1176 - 1203

## Overview
Locates the nearest unprocessed ExtensionVersionInfo vertex during extension version dependency resolution using Dijkstra's algorithm.

## Definition


## Detailed Description
This function implements part of Dijkstra's shortest path algorithm for finding extension upgrade paths. It searches through a list of ExtensionVersionInfo vertices to find the one with the smallest distance value among those that haven't been processed yet (distance_known = false). The algorithm is O(N^2) complexity as noted in the comments - a priority queue implementation would be more efficient but is not currently needed.

The function is a key component in PostgreSQL's extension version resolution system, helping determine the optimal upgrade path between extension versions by finding the next closest vertex to process in the dependency graph.

## Parameters / Member Variables
- : List of ExtensionVersionInfo structures representing extension version vertices in the dependency graph

## Dependencies
- Functions called/Symbols referenced:
  - ExtensionVersionInfo (struct type)
  - lfirst (list iteration macro)
- Called from (representative examples):  
  - find_update_path

## Notes and Other Information
- Part of Dijkstra's algorithm implementation for extension version path finding
- Current O(N^2) implementation could be optimized with priority queue but deemed unnecessary for typical use cases
- Only considers vertices where distance_known is false (unprocessed vertices)
- Returns NULL if no unprocessed vertices remain
- Static function - only used within extension.c module