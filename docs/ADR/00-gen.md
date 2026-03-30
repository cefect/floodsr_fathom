# ADR: general coding rules

- never use `*args` in functions unless explicitly needed.  (includes the bare `*` keyword-only separator)
- when re-raising exceptions with added context, prefer the `e.add_note(...)` pattern so the original exception is preserved and the extra context is attached cleanly.
- when an exception is allowed to propagate, prefer raising it with a clear message at the origin and attach extra run-context later at the handling boundary rather than logging the same failure at multiple layers.

## naming

- private helpers should begin with `_`.
- module constants and registries use `UPPER_CASE`.
