# ADR parameters

Use three distinct configuration layers:
- `parameters.py`  define scientific/default behavior
- `smk/config.yaml` binds environment-specific inputs and user selection filters and data file locations
- `smk/profiles/**` define execution-engine behavior only, such as scheduler/executor choice, cores, retries, global resources, latency knobs, and submission policy
- `conf.py` defines project-level configuration such as scenario dimensions, but not scientific defaults or environment-specific bindings

 

## `smk/config.yaml`

`smk/config.yaml` is the binding layer for a specific run environment and operator selection.

It should hold:
- environment-specific input paths and cache directories
- checkpoint path bindings referenced indirectly through keys such as `weights_config_key`
- user selection filters such as `state_ids`, `sensor_ids`, `item_ids`, and similar workflow selectors
- temporary/operator-facing overrides passed through Snakemake config when explicitly desired for a run

It should not be treated as the canonical home for scientific defaults that belong in `parameters.py` or stage-boundary schema that belongs in contracts.

### `config.get(...)` override pattern

Using `config.get(...)` inside Snakefiles and scripts is a valid pattern for temporary tuning/testing overrides, especially for CLI runs such as:

```bash
snakemake --config myparam=override
```

Those override values do not need to exist in `smk/config.yaml` ahead of time.

This pattern is acceptable when:
- the override is intentionally operator-facing for temporary tuning/testing,
- the default behavior still comes from `parameters.py` and/or contracts,
- and the override path is explicit in code rather than silently replacing the canonical default source.

When the same override becomes stable or scientifically meaningful, it should be promoted into `parameters.py` or contracts according to the ownership rules below.

## `smk/profiles/**`

Snakemake profiles are the execution-policy layer.

Profiles should define only engine/executor behavior, for example:
- scheduler/executor selection
- cores/jobs
- retries / rerun policy
- global resources
- submission throttling and latency behavior
- backend-specific submission defaults

Profiles are not the place for scientific defaults, workflow routing, or stage-boundary contracts.

Snakemake config and profiles are different mechanisms and do not accept the same keys.
They may both be overridden from the CLI, but via different Snakemake interfaces and with different semantics.
