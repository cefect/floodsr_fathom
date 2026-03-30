# ADR 10: Logging for Snakemake Workflows and Scripts

pytest-specific logger usage is defined in [ADR 01](/workspace/docs/ADRs/01-testing.md).
 
## Decision

Keep logging centralized in two places only:
- Snakefiles own workflow-scheduler logging
- rule scripts and shared helpers own job/runtime logging

### Snakefile logging

Snakefiles should use Snakemake's provided logger:
- `from snakemake.logging import logger`
- log workflow spinup, major environment/config decisions, and lifecycle hooks such as `onstart`, `onsuccess`, and `onerror`
- do not create custom file handlers inside Snakefiles

### Rule-script logging

Each Snakemake rule script `__main__` entrypoint should initialize one file-backed logger from `snakemake.log[0]` with:
- `from smk.scripts.coms import get_logger`
- one stable `logger_name` that identifies the rule/run
- one explicit stream level for interactive visibility when a stream handler is enabled
- prefer passing the rule identity explicitly from the Snakefile, for example `params.rule_name`, instead of hard-coding the rule name repeatedly inside the script

The logger created at the entrypoint is the logging authority for that rule run:
- pass it into the stage `main_*` function
- pass it further into helper functions and worker classes through `logger=...`
- shared helpers should accept `logger=None` and fall back to `logging.getLogger(__name__)`

### Handler policy

`smk/scripts/coms.py:get_logger(...)` is the canonical logger factory for Snakemake scripts.

It should provide:
- one file handler at `DEBUG` level writing to `snakemake.log[0]`
- no duplicate handlers when the same logger name is reused in-process
- optional stderr streaming for local/manual runs
- `logger.propagate = False` so file-backed rule logs do not double-write through ancestor handlers

### What to log

Use logs for execution breadcrumbs, not for data products.

Expected `INFO` events:
- rule start / end
- key input and output paths
- major runtime decisions such as selected device, cache mode, or filtered row counts
- concise output summaries

Expected `DEBUG` events:
- parameter dumps and merged runtime config
- per-major-step diagnostics
- loop progress checkpoints when they materially help debugging

Expected `WARNING` events:
- degraded but recoverable behavior
- cache purges, fallbacks, skipped items, or suspicious upstream inputs

### Exception handling

Snakemake script entrypoints should wrap execution in `try/except`, log the failure with `logger.exception(...)`, then re-raise.

Best practice:
- use `logger.exception(...)` at the entrypoint/boundary where the exception is being handled, not at every inner raise site
- if extra runtime context is needed before re-raising, attach it with `e.add_note(...)` and then re-raise the original exception
- inner workflow/stage code should usually raise a clear exception message and let the boundary logger capture the traceback once
- when rule-specific context is needed for logger names, notes, or messages, source it from one explicit rule-name parameter passed by the Snakefile rather than repeating string literals in multiple places

### pytest logging boundary

This ADR does not define pytest fixture wiring.

For tests:
- prefer the shared `logger` fixture from `tests/conftest.py`
- keep pytest logger setup conventions in [ADR 01](/workspace/docs/ADRs/01-testing.md)

### ADR ownership boundary

This ADR owns logging policy.

Other ADRs may mention logging only when needed for stage-specific behavior, and should otherwise cross-reference this ADR instead of redefining logging conventions.
