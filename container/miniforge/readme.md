# US Flood Events dev container


## BASE IMAGE=====================

```bash
#base image
docker run -it --rm condaforge/miniforge3:25.3.1-0 bash

 
 
```
 


## Build Images
from WSL:

### Variables (set once per target)
```bash
# shared
tag="v0.5"
basenm="cefect/floodsr-fathom:miniforge"

# target: deploy
target="deploy"

# target: dev
target="dev"
 

export IMAGE_NAME="${basenm}-${target}-${tag}"
```

### Commands (shared across targets)
```bash
# build image for current target

docker buildx build --load -f container/miniforge/Dockerfile -t "${IMAGE_NAME}" --target "${target}" .

 

# optional: export lock file for current env_name
env_name="deploy"
docker run --rm --entrypoint /bin/bash -v "$PWD/container/miniforge:/out" "${IMAGE_NAME}" \
  bash -lc "conda env list && conda env export -n ${env_name} > /out/conda-env-${target}.lock.yml"
echo "$PWD/container/miniforge/conda-env-${target}.lock.yml"

# optional: export pip freeze for current env_name
docker run --rm --entrypoint /bin/bash -v "$PWD/container/miniforge:/out" "${IMAGE_NAME}" \
  bash -lc "conda run -n ${env_name} python -m pip freeze > /out/pip-freeze-${target}.txt"
echo "$PWD/container/miniforge/pip-freeze-${target}.txt"


# OPTIONAL: push to DockerHub (must be logged in and have permissions to push to the repo)
docker push "${IMAGE_NAME}"



 
yq -y -i --arg dev "$IMAGE_NAME" '.services.dev.image = $dev' .devcontainer/docker-compose.yml


 
```
