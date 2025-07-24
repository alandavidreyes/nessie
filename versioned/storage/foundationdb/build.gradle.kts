/*
 * Copyright (C) 2025 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.tools.ant.taskdefs.condition.Os

plugins { id("nessie-conventions-server") }

publishingHelper { mavenName = "Nessie - Storage - FoundationDB" }

description = "Storage implementation for FoundationDB."

// Configure tests to use dynamic library detection
tasks.withType<Test>().configureEach {
  // Set up JVM args for library detection on different platforms
  val osName = System.getProperty("os.name").lowercase()
  when {
    osName.contains("mac") -> {
      jvmArgs("-Djava.library.path=/usr/local/lib")
    }
    osName.contains("linux") -> {
      jvmArgs("-Djava.library.path=/usr/local/lib:/usr/lib:/usr/lib/x86_64-linux-gnu")
    }
  }
}

// Disable tests on Windows since FoundationDB is not available
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

// Tests will automatically skip if FoundationDB is not available
// No need to disable them at the build level

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-storage-common-serialize"))

  implementation("org.foundationdb:fdb-java:7.4.3")

  // Immutables support
  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

  // Required annotations
  compileOnly(libs.jakarta.annotation.api)

  // Guava for AbstractIterator
  implementation(libs.guava)

  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-common-tests"))

  // Test dependencies
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.slf4j.api)
  testImplementation(libs.logback.classic)

  // Integration test dependencies
  intTestImplementation(project(":nessie-versioned-storage-testextension"))
  intTestImplementation(project(":nessie-versioned-storage-common-tests"))
  intTestImplementation(project(":nessie-versioned-tests"))
  intTestImplementation(platform(libs.junit.bom))
  intTestImplementation(libs.bundles.junit.testing)
  intTestRuntimeOnly(libs.logback.classic)
}
