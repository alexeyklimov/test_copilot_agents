plugins {
    application
}

repositories {
    mavenCentral()
}

val arrowVersion = "19.0.0"
val cassandraDriverVersion = "4.19.3"
val jacksonVersion = "2.21.4"
val jmhVersion = "1.37"
val junitVersion = "6.0.1"
val simulacronVersion = "0.12.0"

val functionalTestSourceSet = sourceSets.create("functionalTest")
val jmhSourceSet = sourceSets.create("jmh")
val loadTestSourceSet = sourceSets.create("loadTest")

configurations[functionalTestSourceSet.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[functionalTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())
configurations[jmhSourceSet.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[jmhSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())
configurations[loadTestSourceSet.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[loadTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

functionalTestSourceSet.compileClasspath += sourceSets.main.get().output
functionalTestSourceSet.runtimeClasspath += sourceSets.main.get().output
functionalTestSourceSet.compileClasspath += sourceSets.test.get().output
functionalTestSourceSet.runtimeClasspath += sourceSets.test.get().output
jmhSourceSet.compileClasspath += sourceSets.main.get().output
jmhSourceSet.runtimeClasspath += sourceSets.main.get().output
jmhSourceSet.compileClasspath += sourceSets.test.get().output
jmhSourceSet.runtimeClasspath += sourceSets.test.get().output
loadTestSourceSet.compileClasspath += sourceSets.main.get().output
loadTestSourceSet.runtimeClasspath += sourceSets.main.get().output
loadTestSourceSet.compileClasspath += sourceSets.test.get().output
loadTestSourceSet.runtimeClasspath += sourceSets.test.get().output

dependencies {
    implementation("org.apache.cassandra:java-driver-core:$cassandraDriverVersion")
    implementation("org.apache.cassandra:java-driver-query-builder:$cassandraDriverVersion")
    implementation("org.apache.arrow:arrow-memory-unsafe:$arrowVersion")
    implementation("org.apache.arrow:arrow-vector:$arrowVersion")
    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonVersion")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:4.0.0-M1")
    testImplementation("com.datastax.oss.simulacron:simulacron-native-server:$simulacronVersion")

    add(functionalTestSourceSet.implementationConfigurationName, "org.junit.jupiter:junit-jupiter:$junitVersion")
    add(functionalTestSourceSet.runtimeOnlyConfigurationName, "org.junit.platform:junit-platform-launcher")
    add(functionalTestSourceSet.implementationConfigurationName, "org.assertj:assertj-core:4.0.0-M1")
    add(functionalTestSourceSet.implementationConfigurationName, "com.datastax.oss.simulacron:simulacron-native-server:$simulacronVersion")

    add(jmhSourceSet.implementationConfigurationName, "org.openjdk.jmh:jmh-core:$jmhVersion")
    add(jmhSourceSet.annotationProcessorConfigurationName, "org.openjdk.jmh:jmh-generator-annprocess:$jmhVersion")
    add(jmhSourceSet.runtimeOnlyConfigurationName, "org.junit.platform:junit-platform-launcher")

    add(loadTestSourceSet.implementationConfigurationName, "org.junit.jupiter:junit-jupiter:$junitVersion")
    add(loadTestSourceSet.runtimeOnlyConfigurationName, "org.junit.platform:junit-platform-launcher")
    add(loadTestSourceSet.implementationConfigurationName, "org.assertj:assertj-core:4.0.0-M1")
    add(loadTestSourceSet.implementationConfigurationName, "com.datastax.oss.simulacron:simulacron-native-server:$simulacronVersion")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

application {
    mainClass = "com.github.alexeyklimov.featurestore.FeatureStoreApplication"
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Darrow.memory.allocation.manager.type=Unsafe"
    )
}

fun Test.configureJvm() {
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--enable-native-access=ALL-UNNAMED"
    )
    systemProperty("arrow.memory.allocation.manager.type", "Unsafe")
    failOnNoDiscoveredTests = false
    useJUnitPlatform()
}

tasks.named<Test>("test") {
    configureJvm()
}

fun JavaExec.configureJvm() {
    jvmArgs(
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--enable-native-access=ALL-UNNAMED",
            "-Darrow.memory.allocation.manager.type=Unsafe"
    )
}

val functionalTest = tasks.register<Test>("functionalTest") {
    description = "Runs functional tests backed by Simulacron."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs = functionalTestSourceSet.output.classesDirs
    classpath = functionalTestSourceSet.runtimeClasspath
    mustRunAfter(tasks.test)
    configureJvm()
}

val loadTest = tasks.register<Test>("loadTest") {
    description = "Runs load tests backed by Simulacron."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs = loadTestSourceSet.output.classesDirs
    classpath = loadTestSourceSet.runtimeClasspath
    mustRunAfter(functionalTest)
    configureJvm()
}

val jmh = tasks.register<JavaExec>("jmh") {
    description = "Runs JMH benchmarks for the read path."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    classpath = jmhSourceSet.runtimeClasspath
    mainClass = "org.openjdk.jmh.Main"
    dependsOn(tasks.named(jmhSourceSet.classesTaskName))
    notCompatibleWithConfigurationCache("Custom benchmark argument wiring reads Gradle providers at execution time.")
    configureJvm()

    val resultFile = providers.gradleProperty("jmh.resultFile")
            .orElse(layout.buildDirectory.file("reports/jmh/results.json").map { it.asFile.absolutePath })
    val benchmarkProfile = providers.gradleProperty("benchmark.profile").orElse("default")
    val includePattern = providers.gradleProperty("jmh.include").orNull

    doFirst {
        file(resultFile.get()).parentFile.mkdirs()
    }

    args("-rf", "json", "-rff", resultFile.get())
    if (benchmarkProfile.get() == "ci") {
        args("-wi", "1", "-i", "1", "-w", "300ms", "-r", "300ms", "-f", "1")
        args(
                "-p", "sliceCount=2",
                "-p", "entitiesPerSlice=25",
                "-p", "featuresPerSlice=32",
                "-p", "entitySizeBytes=128",
                "-p", "valueSizeBytes=256"
        )
    } else {
        args("-wi", "2", "-i", "3", "-w", "1s", "-r", "1s", "-f", "1")
    }
    if (includePattern != null) {
        args(includePattern)
    }
}

tasks.register<JavaExec>("heapSaturationBenchmark") {
    description = "Runs heap saturation throughput benchmark in a dedicated JVM."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    classpath = loadTestSourceSet.runtimeClasspath
    mainClass = "com.github.alexeyklimov.featurestore.HeapSaturationBenchmarkMain"
    dependsOn(tasks.named(loadTestSourceSet.classesTaskName))
    notCompatibleWithConfigurationCache("Dedicated benchmark JVM configuration is dynamic.")
    configureJvm()

    val heapProfile = providers.gradleProperty("heap.profile").orElse("default")
    maxHeapSize = providers.gradleProperty("heap.maxHeap")
            .orElse(if (heapProfile.get() == "ci") "256m" else "512m")
            .get()
    if (heapProfile.get() == "ci") {
        systemProperty("heap.maxConcurrency", "12")
        systemProperty("heap.concurrencyStep", "2")
        systemProperty("heap.requestsPerWorker", "8")
        systemProperty("heap.entitiesPerSlice", "24")
        systemProperty("heap.featuresPerSlice", "24")
        systemProperty("heap.valueSizeBytes", "512")
    }
}

tasks.check {
    dependsOn(functionalTest)
    dependsOn(loadTest)
}
