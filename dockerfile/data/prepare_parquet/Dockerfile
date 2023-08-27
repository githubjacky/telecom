FROM python:3.11.4-bookworm as python-base
WORKDIR /telecom


# dependencies to install poetry and set up ODBC
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    build-essential curl sshpass \
    unixodbc odbcinst default-mysql-server odbc-mariadb

RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.6.0


FROM python-base
ENV PATH="$POETRY_HOME/bin:$PATH"
# no need to create virtual environment since the docker containr is already is
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false

COPY pyproject.toml poetry.lock ./
RUN poetry install --only data
EXPOSE 8888

# set up ODBC
COPY dockerfile/data/utils/odbcinst.ini /etc/odbcinst.ini
COPY dockerfile/data/utils/ssh_config /etc/ssh/ssh_config

CMD ["bash"]
