FROM python:3.10.13-bookworm

ARG UID
ARG GID
ARG USER

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential dkms software-properties-common \
        wget curl default-mysql-server git automake libtool libnuma-dev firefox-esr

ENV LD_LIBRARY_PATH="/usr/local/cuda-12.2/lib64/:$LD_LIBRARY_PATH"
ENV PATH="/usr/local/cuda-12.2/bin:$PATH"
COPY cuda-12.2 /usr/local/cuda-12.2

RUN git clone https://github.com/openucx/ucx \
    && cd ucx \
    && ./autogen.sh \
    && mkdir build \
    && cd build \
    && ../contrib/configure-release --with-cuda=/usr/local/cuda-12.2 --enable-mt \
    && make -j8 \
    && make install

RUN groupadd --gid $GID $USER \
    && useradd --uid $UID --gid $GID -m $USER \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USER ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USER \
    && chmod 0440 /etc/sudoers.d/$USER

RUN pip install \
    --extra-index-url=https://pypi.nvidia.com \
    cudf-cu12==24.2.* dask-cudf-cu12==24.2.* cuml-cu12==24.2.* \
    cugraph-cu12==24.2.* cuspatial-cu12==24.2.* cuproj-cu12==24.2.* \
    cuxfilter-cu12==24.2.* cucim-cu12==24.2.* pylibraft-cu12==24.2.* \
    raft-dask-cu12==24.2.*


ENV PATH="/home/$USER/.local/bin:$PATH"
RUN git clone https://github.com/rapidsai/ucx-py \
    && cd ucx-py \
    && pip install -v .

# no need to create virtual environment since the docker containr is already is
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN apt-get update && \
    apt-get install --no-install-recommends -y python3-distutils
RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.6.0
COPY pyproject.toml ./
RUN poetry install

RUN pip install dask-cuda

EXPOSE 8888
EXPOSE 6666
EXPOSE 3306

USER $USER
WORKDIR /home/$USER/telecom

CMD ["bash"]
