#!/usr/bin/env bash
set -euo pipefail

JDK21_HOME="/home/terrance.lzm/.jdks/azul-21.0.9"

if [ ! -x "$JDK21_HOME/bin/java" ]; then
    echo "ERROR: JDK 21 not found at $JDK21_HOME"
    exit 1
fi

echo "Using JDK: $($JDK21_HOME/bin/java -version 2>&1 | head -1)"
echo "Maven: $(mvn --version 2>&1 | head -1)"
echo ""

export JAVA_HOME="$JDK21_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

mvn -T4 clean install \
    -Prelease-all \
    -DskipTests \
    -Dspotbugs.skip=true \
    -Djacoco.skip=true \
    -Drevision=5.5.0-JDK21-SNAPSHOT
