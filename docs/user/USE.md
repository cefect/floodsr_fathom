# Fathom FloodSR Pipeline

This guide explains what this project does and how to run it. It is written for users who are comfortable copying shell commands but may be new to Python, [`floodsr`](https://github.com/cefect/floodsr), or [Snakemake](https://snakemake.readthedocs.io/en/stable/).

Commands below use `bash`. If you use `zsh`, `fish`, PowerShell, or another shell, the commands may need small syntax changes.

## Background

### What problem does this project solve?

This project takes coarse Fathom flood-depth rasters and turns them into higher-resolution, terrain-informed outputs.

In simple terms:

- the input is a low-resolution flood depth raster
- the project fetches matching high-resolution terrain data
- [`floodsr`](https://github.com/cefect/floodsr) uses the terrain to reconstruct a finer flood surface

This is often called **super-resolution** or **resolution enhancement**.

### What are Fathom flood rasters?

[Fathom](https://www.fathom.global/) produces large-scale flood hazard products. In this repo, the low-resolution source data are Fathom depth tiles and companion tile-index files. The workflow expects:

- a directory of raster tiles, configured as `fathom_tiles_dir`
- a directory of GeoPackage tile indexes, configured as `fathom_index_dir`

contact fathom sales to get your aws access and data download instructions

see the sister project [`fathom_aws_fetch`](https://github.com/cefect/fathom_aws_fetch) for scripts that can fetch both of those from AWS and prepare them for this workflow.

The file names encode the scenario dimensions used by the workflow:

- `hazard_type`: `FLUVIAL`, `PLUVIAL`, `COASTAL`
- `protection`: `DEFENDED`, `UNDEFENDED`
- `return_period`: values like `1in5`, `1in100`, `1in1000`
- `tileID`: a tile name like `n49w124`

See `misc/test_data` for some tools to create a small test data set from the AWS source data.



### What is HRDEM?

For Canadian tiles, this workflow fetches terrain from the [High Resolution Digital Elevation Model (HRDEM) Mosaic](https://open.canada.ca/data/en/dataset/0fe65119-e96e-4a57-8bfe-9d9245fba06b), published by Natural Resources Canada. A DEM is a raster of ground elevation. `floodsr` uses that terrain context to place water more realistically on a fine grid.

### What is Snakemake, and why is it used here?

[Snakemake](https://snakemake.readthedocs.io/en/stable/) is a workflow runner. You tell it the output you want, and it figures out which intermediate steps are needed.

That matters here because the project is not one single command. It is a repeatable 3-stage pipeline:

1. `r01_prep`: preprocess one low-resolution Fathom tile
2. `r02_hrdem`: fetch matching HRDEM terrain for that tile
3. `r03_tohr`: run `floodsr` to create the high-resolution result

Snakemake handles:

- dependency ordering
- repeated runs across many scenarios and tiles
- logs per job
- cached intermediate work
- selective reruns when inputs or parameters change

### What is `floodsr` doing inside this repo?

[`floodsr`](https://github.com/cefect/floodsr) is the flood super-resolution engine. Its own [documentation project](https://app.readthedocs.org/projects/floodsr/) describes it as a tool that takes a low-resolution depth raster plus a high-resolution DEM and reconstructs a higher-resolution flood raster.

This repo uses the `floodsr` **Python API** inside Snakemake rule scripts instead of calling the CLI directly for each step. That is useful because Snakemake passes Python objects, file paths, logs, and config values directly into the stage functions.

The current workflow uses:

- model version `ResUNet_16x_DEM`
- default minimum retained depth `0.01 m`

but these are configurable in `smk/config.yaml` and can be overridden on the command line.

### Project objective

The goal of this repo is to run `floodsr` reproducibly on many Fathom scenarios and tiles, while keeping:

- inputs traceable
- outputs organized
- logs easy to inspect
- reruns predictable

## Use

### Before you start

Run all commands from the repository root.

For environment setup options, see the main project [readme](../../readme.md).

 

### Data acquisition and configuration

The main runtime configuration lives in `smk/config.yaml`, but in normal use you create that file yourself from the template shown in the main project [readme](../../readme.md). That means the paths in this guide are examples, and you can choose your own `out_dir` and input locations when you create the file.

The most important settings are:

- `fathom_tiles_dir`: root folder containing the low-resolution Fathom raster tiles
- `fathom_index_dir`: folder containing the scenario GeoPackage index files
- `out_dir`: where workflow outputs, logs, and caches are written
- `debug`: when `true`, allows a small per-scenario subset run
- `model_version`: which `floodsr` model to use in stage `r03_tohr`

This project expects your data preparation step to produce both the tile rasters and the tile index files. 

### Snakemake in one minute

You usually run Snakemake by asking for a final file, not by calling each Python script yourself.

Example:

- asking for `workflow_outdir/03_tohr/.../r03_tohr.vrt` tells Snakemake to build the final high-resolution raster
- Snakemake then runs `r01_prep` and `r02_hrdem` first if they are needed

This project is set up to use the Snakemake profile in `smk/profiles`, so export that once per shell session:

```bash
export SNAKEMAKE_PROFILE=smk/profiles
```

### Prove the environment

This quick check confirms that the two main CLIs are available in the current environment:

```bash
export SNAKEMAKE_PROFILE=smk/profiles
python -m snakemake --help >/dev/null
floodsr --help >/dev/null
```

If both commands return without an error, the basic command-line tooling is available.

### Dry-run the pipeline

A dry-run shows what Snakemake **would** do without writing the final outputs. This is the safest way to learn the workflow.

```bash
export SNAKEMAKE_PROFILE=smk/profiles
snakemake -n workflow_outdir_docproof/03_tohr/PLUVIAL/DEFENDED/1in1000/n49w124/r03_tohr.vrt \
  --config out_dir=workflow_outdir_docproof debug=true tile_cnt=1 \
  hazard_type=PLUVIAL protection=DEFENDED return_period=1in1000
```

What to look for in the output:

- `Job stats` shows how many jobs will run
- `rule r01_prep`, `rule r02_hrdem`, `rule r03_tohr` show the planned stages
- `input`, `output`, and `log` lines show the exact files involved
- `This was a dry-run` confirms that no real processing happened

If Snakemake says `Nothing to be done`, the requested outputs already exist and are up to date for that `out_dir`.

### Run one tile end to end

This command requests one final high-resolution output. Snakemake will run any missing upstream steps automatically.

```bash
export SNAKEMAKE_PROFILE=smk/profiles
snakemake workflow_outdir/03_tohr/PLUVIAL/DEFENDED/1in1000/n49w124/r03_tohr.vrt
```

This is usually the best first real run because:

- it is small
- it exercises the whole pipeline
- it produces one easy-to-inspect output tree

### Run one stage only

If you want to debug the pipeline step by step, you can request an intermediate output instead of the final one.

```bash
export SNAKEMAKE_PROFILE=smk/profiles
snakemake workflow_outdir/01_prep/PLUVIAL/DEFENDED/1in1000/n49w124/r01_prep.tif  # preprocess one low-resolution Fathom tile
snakemake workflow_outdir/02_hrdem/PLUVIAL/DEFENDED/1in1000/n49w124/r02_hrdem.vrt  # fetch the matching high-resolution DEM
snakemake workflow_outdir/03_tohr/PLUVIAL/DEFENDED/1in1000/n49w124/r03_tohr.vrt  # run floodsr and write the final high-resolution output
```

### Run a filtered selection by editing `smk/config.yaml`

If you plan to rerun the same subset many times, put the filters in your local `smk/config.yaml`.

Typical temporary learning setup:

```yaml
debug: true
tile_cnt: 1
hazard_type: PLUVIAL
protection: DEFENDED
return_period: 1in1000
```

Then run an `_all` rule:

```bash
export SNAKEMAKE_PROFILE=smk/profiles
snakemake -n r03_tohr_all
snakemake r03_tohr_all
```

The `_all` rules are convenience targets that expand across all rows remaining after filtering.

### Run a filtered selection with CLI overrides

CLI overrides are useful when you want a one-off run without editing your local `smk/config.yaml`.

```bash
export SNAKEMAKE_PROFILE=smk/profiles
snakemake -n r03_tohr_all \
  --config debug=true tile_cnt=1 hazard_type=PLUVIAL \
  protection=DEFENDED return_period=1in1000 out_dir=workflow_outdir_small
```

Then run it for real:

```bash
export SNAKEMAKE_PROFILE=smk/profiles
snakemake r03_tohr_all \
  --config debug=true tile_cnt=1 hazard_type=PLUVIAL \
  protection=DEFENDED return_period=1in1000 out_dir=workflow_outdir_small
```

CLI overrides take precedence over the values in your local `smk/config.yaml` for that command only.

### Where to find inputs, outputs, logs, and cache

If you use the example template from the README unchanged, outputs go under `workflow_outdir/`, because that template sets `out_dir: "workflow_outdir"`. You can change `out_dir` to any other directory name when you create your own `smk/config.yaml`.

The main stage folders are:

- `workflow_outdir/01_prep/.../r01_prep.tif`
- `workflow_outdir/02_hrdem/.../r02_hrdem.vrt`
- `workflow_outdir/03_tohr/.../r03_tohr.vrt`

Each job also writes a stage log beside the output:

- `workflow_outdir/<stage>/<hazard_type>/<protection>/<return_period>/<tileID>/snake.log`

Project-local cache files live under:

- `workflow_outdir/.cache/`

That cache includes things like:

- the loaded tile index cache
- fetched HRDEM tiles
- cached `floodsr` model weights

### How to read the three workflow stages

- `r01_prep` reads a Fathom tile, converts depths into meters, removes invalid cells, and zeros out dry or very shallow cells
- `r02_hrdem` fetches or assembles a high-resolution DEM aligned to the prepared low-resolution tile
- `r03_tohr` resolves the `floodsr` model, reads the prepared flood raster plus HRDEM, and writes the final high-resolution output

## FAQ / Support

### I am new to Snakemake. What should I do first?

Start with:

1. export `SNAKEMAKE_PROFILE=smk/profiles`
2. run one dry-run for a single target
3. run one real single-tile target
4. inspect the output folder and `snake.log`

### Why not just run `floodsr tohr` directly?

You can, and the package supports that. But this repo is built for repeated batch processing of many Fathom tiles and scenarios. Snakemake is what makes the multi-step, multi-tile workflow reproducible.

### What if I want a clean experiment?

Use a different `out_dir` with `--config out_dir=...`. That keeps your test run separate from any existing outputs.

### Where should I ask for help or report a bug?

Open an issue in the project repository: <https://github.com/cefect/floodsr_fathom/issues>
