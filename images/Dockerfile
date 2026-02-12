# syntax=docker/dockerfile:1

ARG BASE_IMAGE=savannah.ornl.gov/olcf-container-images/cpe:25.03_gnu_ubuntu
FROM ${BASE_IMAGE}

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Pin versions for reproducibility (override with --build-arg if you want)
ARG MPICH_VERSION=3.4.2
ARG FLUX_CORE_VERSION=0.82.0
ARG FLUX_SCHED_VERSION=0.49.0
ARG MATENSEMBLE_VERSION=0.1.2

ENV DEBIAN_FRONTEND=noninteractive \
    MPICH_PREFIX=/opt/mpich \
    FLUX_PREFIX=/opt/flux \
    VENV=/opt/venv

# install dependencies for flux-core and flux-sched
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl wget git \
      build-essential pkg-config \
      autoconf automake libtool \
      cmake ninja-build \
      python3 python3-venv python3-dev python3-pip \
      libffi-dev libssl-dev \
      libzmq3-dev uuid-dev libjansson-dev liblz4-dev libarchive-dev libhwloc-dev libsqlite3-dev \
      libedit-dev libyaml-cpp-dev libboost-dev libboost-graph-dev \
      python3-yaml \
      lua5.4 liblua5.4-dev \
      # runtime libs often needed by big scientific wheels (safe to include)
      libgl1 libx11-6 libxext6 libxrender1 libsm6 \
    && rm -rf /var/lib/apt/lists/*


# build and install flux-core
RUN cd /tmp \
 && curl -fsSL "https://github.com/flux-framework/flux-core/releases/download/v${FLUX_CORE_VERSION}/flux-core-${FLUX_CORE_VERSION}.tar.gz" -o flux-core.tar.gz \
 && tar -xzf flux-core.tar.gz \
 && cd "flux-core-${FLUX_CORE_VERSION}" \
 && ./configure --prefix="${FLUX_PREFIX}" --sysconfdir=/etc --localstatedir=/var \
 && make -j"$(nproc)" \
 && make install \
 && ldconfig \
 && rm -rf /tmp/flux-core*

# build and install flux-sched
ENV PKG_CONFIG_PATH="${FLUX_PREFIX}/lib/pkgconfig" \
    CMAKE_PREFIX_PATH="${FLUX_PREFIX}"

RUN cd /tmp \
 && curl -fsSL "https://github.com/flux-framework/flux-sched/releases/download/v${FLUX_SCHED_VERSION}/flux-sched-${FLUX_SCHED_VERSION}.tar.gz" -o flux-sched.tar.gz \
 && tar -xzf flux-sched.tar.gz \
 && cd "flux-sched-${FLUX_SCHED_VERSION}" \
 && cmake -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="${FLUX_PREFIX}" \
 && cmake --build build \
 && cmake --install build \
 && ldconfig \
 && rm -rf /tmp/flux-sched*

# install matensemble
ENV PATH="${VENV}/bin:${FLUX_PREFIX}/bin:${MPICH_PREFIX}/bin:${PATH}" \
    LD_LIBRARY_PATH="${FLUX_PREFIX}/lib:${MPICH_PREFIX}/lib:${LD_LIBRARY_PATH}" \
    MPICC="${MPICH_PREFIX}/bin/mpicc" \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python3 --version \
 && python3 -m venv "${VENV}" \
 && pip install -U pip wheel setuptools \
 && pip install --no-cache-dir \
      -i https://test.pypi.org/simple \
      "matensemble[flux]==${MATENSEMBLE_VERSION}" \
      --extra-index-url https://pypi.org/simple

# sanity checks
RUN flux --version \
 && python -c "import matensemble; import flux; import mpi4py; print('imports ok')"

WORKDIR /work
CMD ["/bin/bash"]

