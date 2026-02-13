# Dockerfile
FROM matensemble-base:latest

SHELL ["/bin/bash", "-lc"]

# --- install uv ---
RUN curl -LsSf https://astral.sh/uv/install.sh \
  | env UV_INSTALL_DIR=/usr/local/bin sh \
 && /usr/local/bin/uv --version

# --- clone MatEnsemble ---
RUN mkdir -p /opt/src \
 && rm -rf /opt/src/MatEnsemble \
 && git clone --depth 1 https://github.com/FredDude2004/MatEnsemble.git /opt/src/MatEnsemble

WORKDIR /opt/src/MatEnsemble

# --- install all deps + matensemble into /opt/venv ---
# Matches: export UV_PROJECT_ENVIRONMENT=/opt/venv ; uv sync --all-extras --dev --no-editable
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

RUN uv sync --all-extras --dev --no-editable

# Apptainer %runscript: flux start
ENTRYPOINT ["flux", "start"]
