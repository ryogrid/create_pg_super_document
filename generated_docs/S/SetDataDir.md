# SetDataDir

## Location
src/backend/utils/init/miscinit.c: 435 - 454

## Overview
Sets the PostgreSQL data directory path, ensuring it is converted to an absolute path for consistent file system operations.

## Definition
```c
void SetDataDir(const char *dir)
```

## Detailed Description
SetDataDir is the canonical function for setting the global DataDir variable in PostgreSQL. It ensures that all data directory paths are stored as absolute paths, regardless of whether the input is relative or absolute. This normalization is crucial for preventing path-related issues that could arise from changing working directories during PostgreSQL's operation. The function properly manages memory by freeing any previously set DataDir before assigning the new path, preventing memory leaks in scenarios where the data directory might be changed multiple times during initialization.

## Parameters / Member Variables
- `dir`: A null-terminated string containing the file system path to the data directory (can be relative or absolute)

## Dependencies
- Functions called/Symbols referenced:
  - make_absolute_path (converts relative paths to absolute paths)
  - free (deallocates previously allocated DataDir memory)
  - Assert (validates input parameter)
- Called from (representative examples):
  - restore_backend_variables
  - SelectConfigFiles
  - AmSpecialWorkerProcess

## Notes and Other Information
This function is the recommended way to set the DataDir global variable, as indicated by the source comment "Use this, never set DataDir directly." The absolute path conversion is essential for PostgreSQL's portability and reliability, especially in environments where the working directory may change. The function's memory management ensures that multiple calls won't leak memory, making it safe to use during configuration processing where the data directory path might be refined through multiple steps.