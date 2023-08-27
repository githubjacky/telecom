FROM julia:1.9.3-bookworm
WORKDIR telecom


COPY Project.toml Manifest.toml .

RUN julia --project=. -e "using Pkg; Pkg.instantiate()"
RUN mkdir -p data/processed

# for odbc library
ENV OVERRIDE_ODBCJL_CONFIG=true

CMD ["bash"]
