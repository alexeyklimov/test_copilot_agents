FROM eclipse-temurin:25-jdk

WORKDIR /workspace

COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY gradle gradle
COPY settings.gradle.kts settings.gradle.kts
COPY gradle.properties gradle.properties
COPY README.md README.md
COPY app app
COPY scripts scripts

RUN chmod +x gradlew scripts/compare_jmh.py

ENTRYPOINT ["./gradlew"]
CMD ["jmh", "-Pbenchmark.profile=ci", "heapSaturationBenchmark", "-Pheap.profile=ci"]
