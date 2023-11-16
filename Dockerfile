FROM python:3.10.13-bookworm

ARG UID
ARG GID
ARG USER

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential dkms software-properties-common \
    wget curl default-mysql-server git automake libtool libnuma-dev firefox-esr

ENV LD_LIBRARY_PATH="/usr/local/cuda-12.2/lib64/"
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
    cudf-cu12 dask-cudf-cu12 cuml-cu12 cugraph-cu12 cuspatial-cu12 cuproj-cu12 cuxfilter-cu12 cucim

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



EXPOSE 8888
EXPOSE 6666
EXPOSE 3306

USER $USER
      e /usr/local/cuda-12.2/lib64:/usr/local/cuda-12.2/lib64
WORKDIR /home/$USER/telecom

CMD ["bash"]
