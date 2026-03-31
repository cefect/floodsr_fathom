# ADR 11: Caching for Snakemake Workflows and Tests

## Decision

Use one explicit cache ownership pattern across the Snakefile, rule scripts, and pytest runs.

### Snakefile cache ownership

- the Snakefile owns project-local cache directory resolution for workflow runs
- resolve the project cache root in the Snakefile from `out_dir`, typically as `{out_dir}/.cache`
- shared Snakefile helpers such as `load_index(...)` should receive their cache directory from that resolved project cache root rather than recomputing their own project cache location
- each rule invocation should resolve its own rule-local cache directory from the project cache root and the rule identity, for example `{out_dir}/.cache/{rule_name}`
- rule-local cache paths should be passed into rule scripts and helpers explicitly through named params when those scripts or helpers need cache control

### Rule-script fallback behavior

- Snakemake rule scripts should prefer the cache directory passed in from the Snakefile
- when a script or helper is run outside the normal Snakefile path or no cache directory is passed, it should fall back to a reasonable system-default cache location rather than failing only because cache wiring is absent
- fallback cache behavior should be explicit in code and easy to inspect during debugging (use log.warning)

### pytest cache isolation

- pytest runs should isolate cache effects from the main project cache (except when the test is explicitly testing cache behavior in the main project cache)
- tests that exercise cache-aware functions or rules should pass a test-local temp directory as the cache directory
- this applies to direct python tests and Snakemake CLI proof tests
- test cache paths should live under the pytest temp tree so cache artifacts are disposable and do not pollute developer caches or canonical workflow outputs


### Ownership boundary

- this ADR owns cache-directory policy for Snakemake workflows and their tests
- ADR 02 should reference this ADR for cache wiring patterns rather than redefining them
- ADR 03 should reference this ADR for cache-dir ownership between `smk/config.yaml`, Snakefiles, and scripts
- ADR 01 should reference this ADR for pytest cache isolation
