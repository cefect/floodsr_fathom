# FloodSR for fathom global flood

- fetching fathom tiles from AWS, see `/home/cefect/LS/09_REPOS/04_TOOLS/fathom_aws_fetch`
- fathom data notes: `l:\05_DATA\_jobs\2407_FHIMP\fathom\readme.md`

## setup
- pull image and enter .devcontainer (see below). TODO: prove on a simple docker run (no dev layer?)
- fetch AWS tiles into a nice directory (see `fathom_aws_fetch`), set this in `smk/config.yaml` as `fathom_tiles_dir`. note: the current local Fathom source tree does not include `PLUVIAL-UNDEFENDED`
- create tile index into a nice directory (see `fathom_aws_fetch`), set this in `smk/config.yaml` as `fathom_index_dir`


### devcontainer
project is setup to use untracked devcontainer configuration for a consistent dev environment. 

create the following files (see below for content) and customize as needed:

devcontainer:
```json
{
  "name": "floodsr_fathom_DC",
  "dockerComposeFile": "./docker-compose.yml",
  //"dockerComposeFile": "./docker-compose_cpu.yml",

  "service": "dev",
  "workspaceFolder": "/workspace",
  "shutdownAction": "stopCompose",
  "forwardPorts": [53682],
  "portsAttributes": {
    "53682": {
      "label": "aws-sso-callback"
    }
  },
  // config codex (mount settings and env var)
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
        "openai.chatgpt", // add codex
        // "ms-azuretools.vscode-containers",
        // "ms-azuretools.vscode-docker",
        "ms-python.debugpy",
        "ms-python.python",
        "ms-python.vscode-pylance",
        //"ms-python.vscode-python-envs",
        "ms-toolsai.jupyter",
        //"ms-toolsai.datawrangler",
        "ms-toolsai.vscode-jupyter-powertoys",
        "vict0rsch.coblock",
        "hbenl.vscode-test-explorer",
        "littlefoxteam.vscode-python-test-adapter",
        //"htcondor.htcondor",
        "snakemake.snakemake-lang",
        "mechatroner.rainbow-csv",
        "google.geminicodeassist",
        //"george-alisson.html-preview-vscode", // for viewing html output of notebooks
      ],
      "settings": {
        "python.defaultInterpreterPath": "/opt/conda/envs/deploy/bin/python",
        "python.terminal.activateEnvironment": false, //already set on the container
        "python.testing.pytestEnabled": true,
        "remote.extensionKind": {
          // "ms-azuretools.vscode-containers": ["ui"],
          // "ms-azuretools.vscode-docker": ["ui"],
          "google.geminicodeassist": ["ui"],
        }
      }
    }
  }
}

```

docker-compose.yml:
```yaml
name: floodsr_fathom_compose
services:
  dev:
    image: cefect/floodsr-fathom:miniforge-dev-v0.5
    env_file:
      - /home/cefect/.config/floodsr_fathom/secrets.compose.env
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
      - /home/cefect/.pypirc:/home/cefect/.pypirc:ro
      - /etc/localtime:/etc/localtime:ro
      - /etc/timezone:/etc/timezone:ro
    working_dir: /workspace
    user: 1000:1000
    ports:
      - 53682:53682
    tty: true
    stdin_open: true
    cpus: 8
    mem_limit: 16g
    pid: host
    command:
      - sleep infinity
```
