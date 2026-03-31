# ADR: Tests

no CI/CD

Cache isolation for pytest and Snakemake proof tests is defined in [ADR 11](/workspace/docs/ADR/11-caching.md).
 
# test categories
Lets divide/mark tests into the following:

## snakemake workflow tests [smk]
These are primiarly to get access to rule scripts.
Tests should mirror the script __main__ entrypoints.
Inputs should be built with `fixtures` to mirror the rule filepath logic. i.e., these **pull directly** from the `workflow_outdir` structure, and therefore **CAN NOT** run without at least the preceeding rule being complete. 
Should be a 1:1 relation between tests and the `main` functions in the rule scripts.

These are organized into test scirpts per workflow.

### CLI proof extension
We also support a second style of snakemake workflow coverage for rule proving, currently implemented in `tests/test_smk.py`.

This is for proving that the real Snakemake CLI can execute a selected rule with realistic workflow paths and selectors, rather than only calling the underlying `main_*` python function.

Contract:
- invoke the real CLI (`python -m snakemake`) rather than importing rule scripts directly
- run in a temp working directory so `out_dir` is not modified
- copy fresh upstream rule outputs from the main `out_dir` into that temp tree before each proof run (except for the first rule proof, which can pull directly from the main `out_dir` since it is the first stage)
- do not chain outputs from one proof test into the next; each rule proof should stage its own upstream inputs from the canonical workflow outputs
- use `--allowed-rules` so the proof stays scoped to the intended rule
- pass pytest temp cache directories into cache-aware workflow code so test caching stays isolated from developer and project caches
 
 

These CLI proof tests complement, rather than replace, the existing direct python workflow tests.

## pytest logging

pytest-based tests should use the shared `logger` fixture from `tests/conftest.py` when the code under test accepts a logger argument.

Contract:
- prefer the shared `logger` fixture over ad hoc per-test logger construction
- pass `logger=logger` or `log=logger` into the function/worker under test where supported
- keep pytest logger setup in `tests/conftest.py`, not duplicated across test modules
- if a test must build an isolated logger, treat that as an exception and keep the reason local and explicit

## Marks
- `smk`: Snakemake workflow tests. These primarily mirror script `__main__` entrypoints, use fixtures that follow workflow filepath logic, and may depend on preceding workflow stages already being complete.
- `short`: Tests expected to run in under 2 seconds.
- `network`: Tests that require network access.


Marks should be applied at the test function level, not with module-level `pytestmark`.
 

 
