plugins {
    id("java")
}

group = "org.example"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.3.1")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:1.7.36")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:1.7.36")

    // https://search.maven.org/artifact/org.opensearch.client/opensearch-rest-high-level-client/1.2.4/jar
    implementation("org.opensearch.client:opensearch-rest-high-level-client:1.3.2")

    // https://search.maven.org/artifact/com.google.code.gson/gson/2.9.0/jar
    implementation("com.google.code.gson:gson:2.9.0")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")

}

tasks.test {
    useJUnitPlatform()
}