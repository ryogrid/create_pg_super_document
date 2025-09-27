"# handle_pm_child_exit_signal

## Simplified Source

```c
// Simplified version of handle_pm_child_exit_signal
static void handle_pm_child_exit_signal(SIGNAL_ARGS) {
    // Set flag to indicate child process has exited
    pending_pm_child_exit = true;

    // Wake up the postmaster main loop to process the exit
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added explanatory comments for the two main actions
- The function is already very simple, so minimal simplification was needed
- This is a signal handler that defers actual processing to the main event loop