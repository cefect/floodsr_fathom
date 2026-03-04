# US Flood Events dev container


## BASE IMAGE=====================

```bash
#base image
docker run -it --rm condaforge/miniforge3:25.3.1-0 bash

 
 
```
 


## Build Images: deploy
from WSL:
```bash

#set the image name
tag="v0.2"
export IMAGE_NAME="cefect/floodsr-fathom:miniforge-deploy-$tag"

# build the container (single shared conda env: deploy)
docker buildx build --load -f container/miniforge/Dockerfile -t "${IMAGE_NAME}" --target deploy .

```


explore w/ a random user
```bash
echo $IMAGE_NAME

# dump installed packages from the shared deploy env
docker run --rm -v "$PWD/container/miniforge:/out" "$IMAGE_NAME" \
  bash -lc "conda env list && \
  conda env export -n deploy > /out/conda-env-deploy.lock.yml"

```

push to Docker Hub
```bash
# push
docker push $IMAGE_NAME

```

## Build Images: dev
from WSL:
```bash
export IMAGE_NAME="cefect/floodsr-fathom:miniforge-dev-$tag"
docker buildx build --load -f container/miniforge/Dockerfile -t "${IMAGE_NAME}" --target dev .
```

check tools in dev target
```bash
docker run --rm --entrypoint /bin/bash "$IMAGE_NAME" -lc \
  "conda env list && aws --version && git lfs version"
```
