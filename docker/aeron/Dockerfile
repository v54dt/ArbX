# Minimal Aeron MediaDriver image. Runs the official aeron-all jar on
# OpenJDK 21 and writes its shared-memory files under AERON_DIR, which is
# bind-mounted from the host so local Rust/Python clients can connect.

FROM eclipse-temurin:21-jre-noble

# Aeron version. Bump in lockstep with rusteron-client's upstream compatibility.
ARG AERON_VERSION=1.48.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /aeron-all.jar \
        "https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar"

ENV AERON_DIR=/dev/shm/aeron

# Aeron needs these --add-opens on Java 17+ because it touches internal
# NIO / reflection APIs. Threading and term-buffer tuned for dev, not prod.
ENTRYPOINT ["java", \
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED", \
    "-Daeron.dir=/dev/shm/aeron", \
    "-Daeron.threading.mode=DEDICATED", \
    "-Daeron.term.buffer.length=8388608", \
    "-cp", "/aeron-all.jar", \
    "io.aeron.driver.MediaDriver"]
