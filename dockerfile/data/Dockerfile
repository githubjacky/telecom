FROM python:3.10-slim

ARG UID
ARG GID
ARG USER

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential curl default-mysql-server

RUN groupadd --gid $GID $USER \
    && useradd --uid $UID --gid $GID -m $USER \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USER ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USER \
    && chmod 0440 /etc/sudoers.d/$USER

RUN pip install \
    --extra-index-url=https://pypi.nvidia.com \
    cudf-cu12 dask-cudf-cu12 cuml-cu12 cugraph-cu12 cuspatial-cu12 cuproj-cu12 cuxfilter-cu12 cucim

# no need to create virtual environment since the docker containr is already is
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.6.0
COPY pyproject.toml ./
RUN poetry install --without model


EXPOSE 8888
EXPOSE 3306

USER $USER
WORKDIR /home/$USER/telecom

CMD ["bash"]
