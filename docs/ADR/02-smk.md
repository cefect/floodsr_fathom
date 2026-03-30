# ADR 02: SMK Snakefile structure 


Workflow-specific ADRs for the Snakemake pipelines live under:
- `docs/ADR/smk/`


## general
- driven by `workflow_outdir/00_tile_index`, with optional config/CLI selection for  `return_period`, `protection`, `hazard_type`. these are the main wildcards. 
- use the below wildcards. use a collector _all rule (after each rule) to invoke all comabtions in the index_df. 
    - hazard_type: FLUVIAL, PLUVIAL, COASTAL
    - protection: DEFENDED, UNDEFENDED
    - return_period: 1in5, 1in10, 1in20
    - tileID: the tileID from the index (e.g., 360_360)
- workflow should invoke and use `snakemake.logging`. this should be passed through and wired into scripts and script runner functions. 
 

## cache and tmp
- use python for retriving the platform agnostic tempdir
- default cache location should be `{out_dir}'/.cache/{ruleName}` where out_dir comes from config and ruleName is the name of the rule (or `00_load_index` for the case of the preabmel load_index f unction)

## rules
- use the folowing rule types
    - normal DAG rules (e.g., r01_hrdem) for the main processing steps.
    - collection/concat _all rules to invoke all combinations of their main rule. should be super short. primarily for dev. 




### normal rules
- always include `tile_index_chunk` in the params as the row from the tile_index_bx for that invocation. 
- include params.DEBUG
- include one explicit `params.rule_name` when the companion script needs the rule identity for logging, exception notes, or similar rule-local messaging
- declare every `input:` and `output:` entry with an explicit named variable. do not rely on positional `snakemake.input[0]` or unnamed `output:` entries in rule definitions.

#### I/O Contract Naming

- name workflow objects from the project-wide object or contract they represent, not from the consuming rule. eg  if `r02_hrdem` reads the raster written by `r01_prep`, keep that object named from `r01_prep` everywhere it appears.
- omit generic direction words such as `input` and `output` from the object variable name. prefer names such as `r01_prep_fp` and `r02_hrdem_fp`.
- keep the object name stable end-to-end: Snakefile `input` and `output` keys, `snakemake.input.<name>` and `snakemake.output.<name>` access, script function parameter names, local variable names, assertions, and emitted filenames should all use the same contract name.
- each distinct workflow object should have one canonical name across the project. the same raster, table, or tile index should not be renamed just because it appears in a downstream rule.
- output filenames should follow the same contract name where practical. for example, the file produced by `r01_prep` should be named `01_prep.tif` and carried as `r01_prep_fp` in downstream rules and scripts.
- rule definitions should still declare every `input:` and `output:` with explicit named variables rather than positional entries.

#### Workflow Naming

- rule-script entrypoints use `main_<stage>_<action>(...)`. keep the stage prefix numeric and zero-padded when the workflow step is numeric.
- ordered workflow methods use a private numeric prefix such as `_<n>_<verb_phrase>(...)` for major ordered phases inside a workflow worker or rule script.

#### Snakefile Rule-Definition Pattern

General convention:
- define one small config/path dictionary per rule
- store it in a shared mapping under a stable key
- have the rule, nearby helper functions, and companion `_all` rules reference that shared entry instead of rebuilding the strings inline
- keep these near the rule for easy readability and maintenance. 

Why:
- keeps rule-local path wiring in one place
- reduces copy/paste drift between `input`, `output`, `log`, `script`, and `expand(...)`
- works around Snakemake parsing/evaluation awkwardness when a later expression needs the same dynamic templates as the rule body

Simple example:

```python
plib = {}

d = {"name": "02_fetch"}
d["od"] = OUT_DIR / d["name"] / "{sensorID}" / "{itemID}"
d["ofp"] = d["od"] / "index.gpkg"
plib["r02"] = d.copy()
del d

rule r02_fetch:
    output:
        r02_fetch_fp = plib["r02"]["ofp"]
    log:
        plib["r02"]["od"] / "snake.log"
    script:
        f"scripts/_{plib['r02']['name']}.py"

rule r02_fetch_all:
    input:
        expand(plib["r02"]["ofp"], sensorID=SENSOR_IDS, itemID=ITEM_IDS)
```

### scripts
- use the main function and `if __name__ == "__main__"` entrypoint patterns from `smk/scripts/example.py`
- when a rule script is only intended to run under Snakemake, keep the Snakemake bootstrap logic directly in the `if __name__ == "__main__"` block rather than adding a separate wrapper function just for that entrypoint
- when a script is paired to one rule, name the main function parameters and the Snakemake bootstrap variables with the same canonical object names declared in the rule. prefer explicit names such as `r01_prep_fp` and `r02_hrdem_fp` over generic names such as `input_fp`, `output_fp`, or positional indexing.
