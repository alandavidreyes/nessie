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
package org.projectnessie.versioned.storage.foundationdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

class TestFoundationDBLibraryUtils {

  @Test
  void testPlatformDetection() {
    // Basic platform detection should never fail
    String platformInfo = FoundationDBLibraryUtils.getPlatformInfo();
    assertThat(platformInfo)
      .isNotNull()
      .contains("Platform:")
      .contains("Java:");
  }

  @Test
  void testLibraryDetection() {
    // Library detection should return a result (even if empty)
    Optional<String> libraryPath = FoundationDBLibraryUtils.detectLibraryPath();
    assertThat(libraryPath).isNotNull();

    // If a library is detected, it should be a reasonable path
    if (libraryPath.isPresent()) {
      String path = libraryPath.get();
      assertThat(path)
        .isNotEmpty()
        .satisfiesAnyOf(
          p -> assertThat(p).endsWith(".dylib"), // macOS
          p -> assertThat(p).endsWith(".so"), // Linux
          p -> assertThat(p).endsWith(".dll") // Windows
        );
    }
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void testLibraryConfiguration() {
    // Save original value to restore later
    String originalValue = System.getProperty("FDB_LIBRARY_PATH_FDB_C");

    try {
      // Clear any existing configuration
      System.clearProperty("FDB_LIBRARY_PATH_FDB_C");

      // Test configuration
      boolean configured = FoundationDBLibraryUtils.configureNativeLibrary();

      if (configured) {
        // If configuration succeeded, the system property should be set
        String configuredPath = System.getProperty("FDB_LIBRARY_PATH_FDB_C");
        assertThat(configuredPath).isNotNull().isNotEmpty();

        // The configured path should match what detection found
        Optional<String> detectedPath =
          FoundationDBLibraryUtils.detectLibraryPath();
        assertThat(detectedPath).isPresent();
        assertThat(configuredPath).isEqualTo(detectedPath.get());
      }
    } finally {
      // Restore original value
      if (originalValue != null) {
        System.setProperty("FDB_LIBRARY_PATH_FDB_C", originalValue);
      } else {
        System.clearProperty("FDB_LIBRARY_PATH_FDB_C");
      }
    }
  }

  @Test
  void testCurrentLibraryPath() {
    // Save original value
    String originalValue = System.getProperty("FDB_LIBRARY_PATH_FDB_C");

    try {
      // Test with no property set
      System.clearProperty("FDB_LIBRARY_PATH_FDB_C");
      Optional<String> currentPath =
        FoundationDBLibraryUtils.getCurrentLibraryPath();
      assertThat(currentPath).isEmpty();

      // Test with property set
      String testPath = "/test/path/libfdb_c.so";
      System.setProperty("FDB_LIBRARY_PATH_FDB_C", testPath);
      currentPath = FoundationDBLibraryUtils.getCurrentLibraryPath();
      assertThat(currentPath).isPresent().contains(testPath);
    } finally {
      // Restore original value
      if (originalValue != null) {
        System.setProperty("FDB_LIBRARY_PATH_FDB_C", originalValue);
      } else {
        System.clearProperty("FDB_LIBRARY_PATH_FDB_C");
      }
    }
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void testLibraryValidation() {
    // This test will only pass if FoundationDB is actually installed
    // But it should never throw exceptions
    boolean isValid = FoundationDBLibraryUtils.validateLibrary();

    // The result depends on whether FoundationDB is installed
    // We can't assert a specific value, but we can ensure it doesn't crash
    assertThat(isValid).isInstanceOf(Boolean.class);

    // If validation succeeded, the library path should be configured
    if (isValid) {
      Optional<String> libraryPath =
        FoundationDBLibraryUtils.getCurrentLibraryPath();
      assertThat(libraryPath).isPresent();
    }
  }

  /**
   * Diagnostic test that prints detailed platform and library information.
   * This is useful for troubleshooting setup issues.
   */
  @Test
  void diagnosticInformation() {
    System.out.println("=== FoundationDB Diagnostic Information ===");
    System.out.println(FoundationDBLibraryUtils.getPlatformInfo());

    boolean isAvailable = FoundationDBLibraryUtils.isFoundationDBAvailable();
    System.out.println(
      "Overall availability: " + (isAvailable ? "AVAILABLE" : "NOT AVAILABLE")
    );

    if (isAvailable) {
      System.out.println("FoundationDB is properly installed and configured.");
    } else {
      System.out.println("FoundationDB is not available on this system.");
      System.out.println("This is expected if FoundationDB is not installed.");
    }

    System.out.println("=================================================");
  }
}
