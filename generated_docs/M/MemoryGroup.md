# MemoryGroup

## Location
src/include/jit/SectionMemoryManager.h: 191 - 226

## Overview
MemoryGroup is a struct within PostgreSQL's JIT compilation system that manages memory allocation and tracking for different types of memory sections (code, read-only data, and read-write data) in the LLVM-based JIT execution engine.

## Definition
# 0 "<stdin>"
# 0 "<built-in>"
# 0 "<command-line>"
# 1 "/usr/include/stdc-predef.h" 1 3 4
# 0 "<command-line>" 2
# 1 "<stdin>"

## Detailed Description
MemoryGroup serves as a container for organizing memory blocks in different states within the SectionMemoryManager. It tracks memory throughout its lifecycle from allocation to finalization. The structure maintains separate collections for:

1. **PendingMem**: Memory blocks that have been allocated to users but haven't yet had their final permissions applied
2. **FreeMem**: Available memory blocks that haven't been allocated to users and don't have permissions applied
3. **AllocatedMem**: All memory blocks that have been requested from the operating system
4. **Near**: A reference memory block used for proximity-based allocation strategies

This organization allows the memory manager to efficiently track memory state transitions and optimize allocation patterns, particularly important for JIT compilation where memory layout can significantly impact performance.

## Parameters / Member Variables
- : Contains memory blocks (subblocks of AllocatedMem) that have been given to users but haven't had their final permissions applied yet
- : Contains memory blocks that are available for allocation and haven't had permissions applied
- : Tracks all memory blocks that have been requested from the system, serving as the master list of allocated memory
- : A memory block used as a reference point for allocating nearby memory, helping with memory locality optimization

## Dependencies
- Functions called/Symbols referenced:
  - [FreeMemBlock](../F/FreeMemBlock.md) (used within FreeMem member)
  - SmallVector (LLVM container type)
  - sys::MemoryBlock (LLVM memory block type)
- Called from (representative examples):
  - applyMemoryGroupPermissions
  - hasSpace

## Notes and Other Information
- This struct is part of PostgreSQL's adaptation of LLVM's SectionMemoryManager for JIT compilation
- The memory manager maintains separate MemoryGroup instances for different allocation purposes (CodeMem, RWDataMem, RODataMem)
- The struct is designed to optimize memory allocation patterns and minimize fragmentation in JIT-compiled code
- Memory transitions through states: allocated → free → pending → finalized with permissions applied
- The Near member helps implement allocation strategies that keep related memory blocks close together for better cache locality