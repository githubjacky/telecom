FROM julia:1.9.2-bookworm
WORKDIR telecom

ENV OVERRIDE_ODBCJL_CONFIG=true
ENV ODBCINI=/root/.odbc.ini
ENV ODBCSYSINI=/etc

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    build-essential ssh sshpass \
    unixodbc odbc-mariadb default-mysql-server

COPY Project.toml .

RUN julia --project=. -e "using Pkg; Pkg.instantiate()"
RUN mkdir -p ./data/processed

# for odbc library
COPY dockerfile/data/utils/odbcinst.ini /etc/odbcinst.ini
COPY dockerfile/data/utils/ssh_config /etc/ssh/ssh_config

CMD ["bash"]
