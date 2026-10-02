FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless maven && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY pyproject.toml README.md LICENSE AUTHORS ChangeLog ./
COPY src/ ./src/
RUN python -m pip wheel --no-deps --wheel-dir /wheels . && python -m pip install --no-cache-dir /wheels/*.whl pymemcache==4.0.0 psutil==7.2.2 py-spy==0.4.1
COPY benchmarks/java /java
RUN cd /java && mvn -q package dependency:copy-dependencies -DskipTests
WORKDIR /benchmarks
COPY benchmarks/*.py ./
ENTRYPOINT ["timeout", "--signal=TERM", "--kill-after=10s", "1200s", "python", "runner.py"]
