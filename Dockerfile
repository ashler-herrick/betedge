# Combined Dockerfile for ThetaTerminal + Data Manager API
FROM eclipse-temurin:21-jre-jammy

# Install UV manually with latest version system-wide
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    cp /root/.local/bin/uv /usr/local/bin/uv

# Install prerequisites including supervisor and dependencies
RUN apt-get update && apt-get install -y \
      supervisor \
      software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python3.11 python3.11-dev python3.11-venv \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Eclipse Temurin Java 21
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/root/.local/bin:${JAVA_HOME}/bin:${PATH}"

# Verify Java 21 and UV installation
RUN java -version && uv --version

# Set up working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY betedge_data/ ./betedge_data/
COPY ThetaTerminal.jar ./

# Copy supervisor configuration
COPY supervisord.conf /etc/supervisor/supervisord.conf

# Install all dependencies using Python 3.11
RUN uv sync --compile-bytecode --python=/usr/bin/python3.11

# Create non-root user
RUN useradd -m -u 1000 betedge && \
    chown -R betedge:betedge /app


# Create directory for ThetaTerminal data
RUN mkdir -p /home/betedge/.theta && \
    chown -R betedge:betedge /home/betedge/.theta

# Create supervisor log directory
RUN mkdir -p /var/log/supervisor && \
    chown -R betedge:betedge /var/log/supervisor

USER betedge

# Expose API port
EXPOSE 8000

# Health check - check both ThetaTerminal and API
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start supervisor which will manage both processes
ENTRYPOINT ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]

