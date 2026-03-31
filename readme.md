# FloodSR for fathom global flood

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Turn coarse global Fathom flood rasters into higher-resolution terrain-informed products with a reproducible Snakemake workflow.

See the user documentation at [docs/user/USE.md](/workspace/docs/user/USE.md).





## setup
this project is distributed as a Git repository because the workflow, tests, configuration, and container/environment definitions all live together and need to stay versioned in sync.

to get started, you will need to:
- clone the repo first for any setup option:
```bash
git clone git@github.com:cefect/floodsr_fathom.git floodsr_fathom
cd floodsr_fathom
```
- setup environment: option 1: devcontainer (recommended), option 2: run with docker; option 3: local conda environment. see below. 
- fetch AWS tiles into a nice directory (see `fathom_aws_fetch`), set this in `smk/config.yaml` as `fathom_tiles_dir`. note: the current local Fathom source tree does not include `PLUVIAL-UNDEFENDED`
- create tile index into a nice directory (see `fathom_aws_fetch`)
- create the `smk/config.yaml` file (see template below) and set

### `smk/config.yaml` template
create a file called `smk/config.yaml` with the following content, and customize the paths and parameters as needed for your setup. This file is used by the Snakemake workflow to locate inputs, set output directories, and configure debug options.
```yaml
fathom_tiles_dir: "_inputs/300x300_2tile/00_tiles"  # Root directory holding the low-resolution Fathom tile folders.
fathom_index_dir: "_inputs/300x300_2tile/00_tile_index"  # Directory holding the scenario GeoPackage tile indexes.
out_dir: "workflow_outdir"  # Workflow output root for all rule products, logs, and caches.

debug: false  # When true, apply tile_cnt after filtering for smaller scenario proof runs.
tile_cnt: None  # Per-scenario tile limit used only when debug=true; None keeps all matching tiles.
model_version: "ResUNet_16x_DEM"  # FloodSR model version used by the r03_tohr rule.
```

## setup environment

### environment option 1: devcontainer
Dev Containers provide a reproducible development environment for VS Code by defining the workspace, runtime, and tooling in config files; see the official landing page at [containers.dev](https://containers.dev/).
This is the most native way to work with the project interactively.

Requirements:
- [VS Code](https://code.visualstudio.com/)
- the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- [Docker Engine or Docker Desktop](https://docs.docker.com/get-docker/)
- local access to the project repo and any host data directories you want to mount into the container

To setup, create the following files (see below for content) and customize as needed, then rebuild and enter the container with [VS Code](https://code.visualstudio.com/docs/devcontainers/containers):

`.devcontainer/devcontainer.json`:
```json
{
  "name": "floodsr_fathom_DC",
  "dockerComposeFile": "./docker-compose.yml",
  //"dockerComposeFile": "./docker-compose_cpu.yml",

  "service": "dev",
  "workspaceFolder": "/workspace",
  "shutdownAction": "stopCompose",

 
  "updateRemoteUserUID": true,
  "containerUser": "cefect",
  "remoteUser": "cefect",

  "containerEnv": {
    "CODEX_HOME": "/home/cefect/.codex"
  },
  // probe login shell env so sourced secrets are visible to VS Code processes
  "userEnvProbe": "loginInteractiveShell",
  "customizations": {
    "vscode": {
      "extensions": [
        "github.copilot",
        "github.copilot-chat",
        "openai.chatgpt", 
        "ms-python.debugpy",
        "ms-python.python",
        "ms-python.vscode-pylance",
        "ms-toolsai.jupyter",
        "ms-toolsai.vscode-jupyter-powertoys",
        "snakemake.snakemake-lang",

      ],
      "settings": {
        "python.defaultInterpreterPath": "/opt/conda/envs/deploy/bin/python",
        "python.terminal.activateEnvironment": false, //already set on the container
        "python.testing.pytestEnabled": true,

      }
    }
  }
}

```

`.devcontainer/docker-compose.yml`:
```yaml
name: floodsr_fathom_compose
services:
  dev:
    image: cefect/floodsr-fathom:miniforge-dev-v0.5
    environment:
      TMPDIR: /home/cefect/LS/10_IO/2407_FHIMP/tmp
      XDG_CONFIG_HOME: /home/cefect/.config
      PYTHONPATH: /workspace
    volumes:
      - /home/cefect/LS/09_REPOS/04_TOOLS/floodsr_fathom:/workspace:delegated
      - /home/cefect/LS/10_IO:/home/cefect/LS/10_IO:delegated
      - /home/cefect/.config:/home/cefect/.config:rw
      - /home/cefect/.ssh:/home/cefect/.ssh:ro
      - /home/cefect/.codex:/home/cefect/.codex:rw
      - /etc/localtime:/etc/localtime:ro
      - /etc/timezone:/etc/timezone:ro
    working_dir: /workspace
    user: 1000:1000
    tty: true
    stdin_open: true
    cpus: 8
    mem_limit: 16g
    pid: host
    command:
      - sleep infinity
```

### environment option 2: run with docker
Docker runs the project inside a standalone container without the VS Code Dev Containers integration; see the official Docker overview at [docs.docker.com/get-started/docker-overview](https://docs.docker.com/get-started/docker-overview/). 
This is useful for batch runs and quick reproducible execution from a shell. 
The main limitation for this project is that you must manage the volume mounts, user mapping, secrets, and local data paths yourself, so interactive editing and VS Code integration are less convenient than the devcontainer setup.

Requirements:
- [Docker Engine or Docker Desktop](https://docs.docker.com/get-docker/)
- permission to run `docker build` and `docker run`
- local access to the project repo and the data roots you plan to mount
- any credentials or local config needed for the external data sources you use

Example commands:
```bash
# Option 1: pull a prebuilt image from Docker Hub if one is published for this project.
docker pull cefect/floodsr-fathom:miniforge-dev-v0.5

# Option 2: build the image locally from this repo.
docker build -f container/miniforge/Dockerfile -t floodsr-fathom:miniforge-dev-v0.5 .

# Start an interactive container with the repo and data mounted in the expected locations.
docker run --rm -it \
  -v /path/to/repo:/workspace \
  -w /workspace \
  floodsr-fathom:miniforge-dev-v0.5 bash

# Run a short local pytest subset inside the container.
conda run -n deploy pytest -q tests/test_snakefile_coms.py tests/test_workflow.py -k 'not network'

# Dry-run one workflow target to confirm Snakemake wiring.
conda run -n deploy snakemake -n workflow_outdir/01_prep/PLUVIAL/DEFENDED/1in1000/n49w124/r01_prep.tif
```

### environment option 3: local conda environment
Conda installs the project dependencies directly into a local Python environment on the host machine; see the official getting-started guide at [docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html). 
This is the lightest setup if you already manage geospatial Python locally. 
The limitation for this project is that GDAL, rasterio, Snakemake, and local path assumptions are more sensitive to host-specific differences, so this is the least reproducible option and usually needs more manual troubleshooting.

Requirements:
- [Conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html), [Miniconda](https://www.anaconda.com/docs/getting-started/miniconda/install), or [Mambaforge](https://github.com/conda-forge/miniforge#mambaforge)
- a shell where `conda activate` works
- enough local disk space for the environment, caches, and test data
- local access to the project repo and required data directories

Example commands:
```bash
conda env create -n fathom_SR -f container/miniforge/environment.yml
conda activate fathom_SR
pytest -q tests/test_snakefile_coms.py tests/test_workflow.py -k 'not network'
snakemake -n workflow_outdir/01_prep/PLUVIAL/DEFENDED/1in1000/n49w124/r01_prep.tif
```
