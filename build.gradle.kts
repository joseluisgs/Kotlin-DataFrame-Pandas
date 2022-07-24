import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.10"
    // Como plugoin por ser el oficial de jetbrains
    kotlin("plugin.dataframe") version "0.8.1"
}

group = "es.joseluisgs"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    // DataFrames de Kotlin Jetbrains
    implementation("org.jetbrains.kotlinx:dataframe:0.8.1")
    implementation("org.jetbrains.kotlinx:dataframe-core:0.8.1")

    // Instalamos la librería Krangl
    implementation("com.github.holgerbrandl:krangl:0.18.1")

}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

// Data Schema generator
// Make IDE aware of the generated code:
kotlin.sourceSets.getByName("main").kotlin.srcDir("build/generated/ksp/main/kotlin/")