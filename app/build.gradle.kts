plugins {
    application
}

repositories {
    mavenCentral()
}

val arrowVersion = "19.0.0"
val cassandraDriverVersion = "4.19.3"
val jacksonVersion = "2.21.4"
val junitVersion = "6.0.1"
val simulacronVersion = "0.12.0"

val functionalTestSourceSet = sourceSets.create("functionalTest")
val loadTestSourceSet = sourceSets.create("loadTest")

configurations[functionalTestSourceSet.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[functionalTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())
configurations[loadTestSourceSet.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[loadTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

functionalTestSourceSet.compileClasspath += sourceSets.main.get().output
functionalTestSourceSet.runtimeClasspath += sourceSets.main.get().output
functionalTestSourceSet.compileClasspath += sourceSets.test.get().output
functionalTestSourceSet.runtimeClasspath += sourceSets.test.get().output
loadTestSourceSet.compileClasspath += sourceSets.main.get().output
loadTestSourceSet.runtimeClasspath += sourceSets.main.get().output
loadTestSourceSet.compileClasspath += sourceSets.test.get().output
loadTestSourceSet.runtimeClasspath += sourceSets.test.get().output

dependencies {
    implementation("org.apache.cassandra:java-driver-core:$cassandraDriverVersion")
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

tasks.check {
    dependsOn(functionalTest)
    dependsOn(loadTest)
}
