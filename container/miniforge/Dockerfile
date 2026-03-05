# syntax=docker/dockerfile:1.5

FROM condaforge/miniforge3:25.3.1-0 AS base
# https://github.com/conda-forge/miniforge-images/blob/master/ubuntu/Dockerfile
# base environment is pre-activated
# these are pre-installed: wget bzip2 ca-certificates, git, tini

# Use bash for RUN and fail on pipe errors.
SHELL ["/bin/bash", "-o", "pipefail", "-lc"]

# Keep apt operations non-interactive across stages.
ENV DEBIAN_FRONTEND=noninteractive

# make permissive to any non-root user
ARG TARGET_DIRS="/workspace /opt/conda"
# single shared conda env used across deploy/dev
ARG CONDA_NAME=deploy

# use a single consolidated environment file
COPY container/miniforge/environment.yml /tmp/environment.yml

# Install base tools.
RUN --mount=type=cache,id=miniforge-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=miniforge-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
        acl \
        ca-certificates \
        curl \
        gh \
        git \
        openssh-client \
        bzip2 \
        xz-utils \
        tar \
        gzip \
        pciutils \
        binutils \
        unzip \
        pipx

# Apply permissive ACLs on shared directories.
RUN set -eux; \
    for dir in $TARGET_DIRS; do \
        install -dm0755 "$dir"; \
        setfacl -b "$dir"; \
        setfacl -m u::rwx,g::rwx,o::rwx "$dir"; \
        setfacl -d -m u::rwx,g::rwx,o::rwx "$dir"; \
    done

# Configure default umask for collaborative writes.
RUN set -eux; \
    echo 'umask 000' >> /etc/bash.bashrc; \
    echo 'umask 000' >> /etc/profile

# Smoke test GitHub CLI auth commands.
RUN set -eux; \
    gh --version; \
    gh auth --help >/dev/null; \
    gh auth status --help >/dev/null

ENV PATH="/root/.local/bin:${PATH}"


# set some base directory env vars helpful for random users
ENV XDG_CONFIG_HOME=/tmp/.config \
    XDG_CACHE_HOME=/tmp/.cache \
    MPLCONFIGDIR=/tmp/.config/matplotlib

ENV PYTHONUNBUFFERED=0
ENV PYTHONPATH=/workspace

WORKDIR /workspace
ENTRYPOINT []
CMD []


# Deploy layer -----------------------------
FROM base AS deploy
ARG CONDA_NAME=deploy

# Build the single shared conda environment.
RUN --mount=type=cache,id=miniforge-conda-pkgs,target=/opt/conda/pkgs,sharing=locked \
    conda config --set channel_priority strict && \
    mamba env create -yv --strict-channel-priority --name ${CONDA_NAME} --file /tmp/environment.yml && \
    conda env config vars set PYTHONPATH=/workspace --name ${CONDA_NAME}




# activate the environment by default for all users
# the miniconda container activates base by default, here we change that to our env
RUN cat <<EOF2 >/etc/profile.d/conda_env.sh
# Activate the env for all login shells.
if [ -f "${CONDA_DIR}/etc/profile.d/conda.sh" ]; then
  . "${CONDA_DIR}/etc/profile.d/conda.sh"
  if [ "\${CONDA_DEFAULT_ENV:-}" != "${CONDA_NAME}" ]; then
    conda activate "${CONDA_NAME}"
  fi
fi
EOF2
RUN echo 'if [ -f /etc/profile.d/conda_env.sh ]; then . /etc/profile.d/conda_env.sh; fi' >> /etc/bash.bashrc




# smoke test snakemake
RUN set -eux; \
    conda run --name ${CONDA_NAME} snakemake --version; \
    conda run --name ${CONDA_NAME} snakemake --help >/dev/null



# flood SR layer -----------------------
FROM deploy AS floodsr
ARG FLOODSR_TAG='v2025.02.26'
ENV PIPX_HOME=/opt/pipx \
    PIPX_BIN_DIR=/usr/local/bin

RUN pipx install --pip-args="--no-cache-dir" "git+https://github.com/cefect/floodsr.git@${FLOODSR_TAG}"

RUN pipx list | grep -q "package floodsr" && \
    floodsr --help >/dev/null


RUN pipx list | grep -q "package floodsr" && \
    floodsr --help >/dev/null


# Snakemake layer ----------------------
 

# Put PuLP's bundled CBC solver on PATH for workflow calls.
# RUN ln -sf /opt/conda/lib/python3.11/site-packages/pulp/solverdir/cbc/linux/64/cbc \
#     /opt/conda/bin/cbc




# DEV target -----------------------------
FROM floodsr AS dev
ARG CONDA_NAME=deploy

# configure my dev user
ARG USERNAME=cefect
ARG USER_UID=1000
ARG USER_GID=1000
ARG OLD_USERNAME=ubuntu

USER root

# Provision user.
RUN set -eux; \
    if id -u "${OLD_USERNAME}" >/dev/null 2>&1; then \
        usermod -l "${USERNAME}" "${OLD_USERNAME}"; \
        usermod -d "/home/${USERNAME}" -m "${USERNAME}"; \
        groupmod -n "${USERNAME}" "${OLD_USERNAME}" || true; \
        usermod -u "${USER_UID}" "${USERNAME}" || true; \
        groupmod -g "${USER_GID}" "${USERNAME}" || true; \
    else \
        groupadd --gid "${USER_GID}" "${USERNAME}"; \
        useradd --uid "${USER_UID}" --gid "${USER_GID}" --create-home --shell /bin/bash "${USERNAME}"; \
    fi

# Install dev tooling.
RUN --mount=type=cache,id=miniforge-apt-cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=miniforge-apt-lists,target=/var/lib/apt/lists,sharing=locked \
    set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
        sudo \
        git \
        git-lfs \
        rsync \
        procps \
        less \
        strace \
        gdb

# Configure passwordless sudo for the dev user.
RUN set -eux; \
    echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-${USERNAME}; \
    chmod 0440 /etc/sudoers.d/90-${USERNAME}

# Install AWS CLI v2.
RUN set -eux; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"; \
    unzip /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install; \
    rm -rf /tmp/aws /tmp/awscliv2.zip

# Ensure writable runtime directories for the dev user.
RUN set -eux; \
    install -d -o ${USER_UID} -g ${USER_GID} -m 0755 /workspace; \
    install -d -o ${USER_UID} -g ${USER_GID} -m 1777 /tmp /var/tmp; \
    mkdir -p /home/${USERNAME}; \
    chown -R ${USER_UID}:${USER_GID} /home/${USERNAME}

# Install starship prompt.
RUN set -eux; \
    wget -qO /tmp/starship.tgz https://github.com/starship/starship/releases/latest/download/starship-x86_64-unknown-linux-gnu.tar.gz; \
    tar -xzf /tmp/starship.tgz -C /usr/local/bin starship; \
    echo 'eval "$(starship init bash)"' >> /etc/bash.bashrc; \
    rm -rf /tmp/starship.tgz

# setup git: ship in a global gitconfig
COPY container/etc-gitconfig /etc/gitconfig
RUN chmod 644 /etc/gitconfig && git lfs install --system

# set default user
USER ${USER_UID}:${USER_GID}

# set default conda environment
# NOTE: because the user already existed, the base-stage changes to their .bashrc need to be propagated
RUN echo ". ${CONDA_DIR}/etc/profile.d/conda.sh && conda activate ${CONDA_NAME}" >> ~/.bashrc

ENTRYPOINT ["/bin/bash", "-lc"]
