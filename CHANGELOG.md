# Changelog

## 0.2.14 - 2026-11-05

### Fixed

- Drop initialized `JoinAll` and `TryJoinAll` outputs correctly on cancellation,
  early errors and unwinding.
- Release the ready-slot lock before invoking a parent waker, allowing reentrant
  wake callbacks without deadlocking.

### Performance

- Share output-ownership tracking between both join implementations. Release
  builds skip bitset allocation for output types that do not need dropping;
  debug builds retain tracking to check initialization invariants.
