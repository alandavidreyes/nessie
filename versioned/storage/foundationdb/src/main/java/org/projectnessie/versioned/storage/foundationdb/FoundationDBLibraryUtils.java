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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Utility class for detecting and configuring FoundationDB native library paths
 * and cluster files across different platforms. This provides a unified way to
 * check FoundationDB availability for both production and test code.
 */
public final class FoundationDBLibraryUtils {

  private FoundationDBLibraryUtils() {
    // Utility class
  }

  /**
   * Detects the platform-specific FoundationDB native library and sets up the system
   * property for the FoundationDB Java client to use.
   *
   * @return true if a library was found and configured, false otherwise
   */
  public static boolean configureNativeLibrary() {
    Optional<String> libraryPath = detectLibraryPath();
    if (libraryPath.isPresent()) {
      System.setProperty("FDB_LIBRARY_PATH_FDB_C", libraryPath.get());
      return true;
    }
    return false;
  }

  /**
   * Checks if FoundationDB is fully available (both cluster file and native libraries).
   * This is the main method that should be used to determine if FoundationDB can be used.
   *
   * @return true if FoundationDB is available and can be used, false otherwise
   */
  public static boolean isFoundationDBAvailable() {
    // First check if a cluster file exists
    if (!isClusterFileAvailable()) {
      return false;
    }

    // Then validate that the library can be loaded and used
    return validateLibrary();
  }

  /**
   * Checks if a FoundationDB cluster file exists in standard locations.
   *
   * @return true if a cluster file is found, false otherwise
   */
  public static boolean isClusterFileAvailable() {
    return getClusterFilePath().isPresent();
  }

  /**
   * Detects the FoundationDB cluster file path from standard locations.
   *
   * @return Optional containing the cluster file path if found, empty otherwise
   */
  public static Optional<String> getClusterFilePath() {
    List<String> possibleClusterFiles = Arrays.asList(
      "/etc/foundationdb/fdb.cluster",
      "/usr/local/etc/foundationdb/fdb.cluster"
    );

    return findFirstExistingFile(possibleClusterFiles);
  }

  /**
   * Detects the FoundationDB native library path for the current platform.
   *
   * @return Optional containing the library path if found, empty otherwise
   */
  public static Optional<String> detectLibraryPath() {
    String osName = System.getProperty("os.name", "").toLowerCase();
    String osArch = System.getProperty("os.arch", "").toLowerCase();

    if (osName.contains("mac")) {
      return detectMacOSLibrary(osArch);
    } else if (osName.contains("linux")) {
      return detectLinuxLibrary(osArch);
    } else if (osName.contains("windows")) {
      return detectWindowsLibrary(osArch);
    }

    return Optional.empty();
  }

  /**
   * Detects FoundationDB library on macOS.
   */
  private static Optional<String> detectMacOSLibrary(String arch) {
    List<String> searchPaths;

    // macOS paths - FoundationDB is typically installed in /usr/local/lib
    searchPaths = Arrays.asList(
      "/usr/local/lib/libfdb_c.dylib",
      "/usr/lib/libfdb_c.dylib"
    );

    return findFirstExistingFile(searchPaths);
  }

  /**
   * Detects FoundationDB library on Linux.
   */
  private static Optional<String> detectLinuxLibrary(String arch) {
    List<String> searchPaths = Arrays.asList(
      "/usr/local/lib/libfdb_c.so",
      "/usr/lib/libfdb_c.so",
      "/usr/lib/x86_64-linux-gnu/libfdb_c.so",
      "/usr/lib64/libfdb_c.so",
      "/lib/libfdb_c.so",
      "/lib64/libfdb_c.so"
    );

    return findFirstExistingFile(searchPaths);
  }

  /**
   * Detects FoundationDB library on Windows.
   * Note: FoundationDB doesn't officially support Windows, but this is here for completeness.
   */
  private static Optional<String> detectWindowsLibrary(String arch) {
    List<String> searchPaths = Arrays.asList(
      "C:\\Program Files\\FoundationDB\\bin\\fdb_c.dll",
      "C:\\Program Files (x86)\\FoundationDB\\bin\\fdb_c.dll",
      System.getProperty("user.home") +
      "\\AppData\\Local\\FoundationDB\\bin\\fdb_c.dll"
    );

    return findFirstExistingFile(searchPaths);
  }

  /**
   * Finds the first existing file from a list of potential paths.
   */
  private static Optional<String> findFirstExistingFile(List<String> paths) {
    return paths
      .stream()
      .filter(path -> Files.exists(Paths.get(path)))
      .findFirst();
  }

  /**
   * Gets information about the current platform and library detection results.
   */
  public static String getPlatformInfo() {
    String osName = System.getProperty("os.name");
    String osArch = System.getProperty("os.arch");
    String javaVersion = System.getProperty("java.version");

    StringBuilder info = new StringBuilder();
    info
      .append("Platform: ")
      .append(osName)
      .append(" (")
      .append(osArch)
      .append(")\n");
    info.append("Java: ").append(javaVersion).append("\n");

    // Cluster file information
    Optional<String> clusterPath = getClusterFilePath();
    if (clusterPath.isPresent()) {
      info
        .append("FoundationDB cluster file: ")
        .append(clusterPath.get())
        .append("\n");
    } else {
      info.append("FoundationDB cluster file: NOT FOUND\n");
      info.append("Searched cluster file locations:\n");
      info.append("  - /etc/foundationdb/fdb.cluster\n");
      info.append("  - /usr/local/etc/foundationdb/fdb.cluster\n");
    }

    Optional<String> libraryPath = detectLibraryPath();
    if (libraryPath.isPresent()) {
      info
        .append("FoundationDB library: ")
        .append(libraryPath.get())
        .append("\n");

      // Check if file actually exists and get additional info
      Path libPath = Paths.get(libraryPath.get());
      if (Files.exists(libPath)) {
        try {
          long size = Files.size(libPath);
          info.append("Library size: ").append(size).append(" bytes\n");
        } catch (Exception e) {
          info
            .append("Library exists but could not read size: ")
            .append(e.getMessage())
            .append("\n");
        }
      } else {
        info.append("WARNING: Library path detected but file does not exist\n");
      }
    } else {
      info.append("FoundationDB library: NOT FOUND\n");
      info.append("Searched paths:\n");

      // Show what paths were searched based on platform
      String osNameLower = osName.toLowerCase();
      if (osNameLower.contains("mac")) {
        info.append("  - /usr/local/lib/libfdb_c.dylib\n");
        info.append("  - /usr/lib/libfdb_c.dylib\n");
      } else if (osNameLower.contains("linux")) {
        info.append("  - /usr/local/lib/libfdb_c.so\n");
        info.append("  - /usr/lib/libfdb_c.so\n");
        info.append("  - /usr/lib/x86_64-linux-gnu/libfdb_c.so\n");
        info.append("  - /usr/lib64/libfdb_c.so\n");
      } else if (osNameLower.contains("windows")) {
        info.append("  - C:\\Program Files\\FoundationDB\\bin\\fdb_c.dll\n");
        info.append(
          "  - C:\\Program Files (x86)\\FoundationDB\\bin\\fdb_c.dll\n"
        );
      }
    }

    // Overall availability
    info
      .append("\nOverall FoundationDB availability: ")
      .append(isFoundationDBAvailable() ? "AVAILABLE" : "NOT AVAILABLE")
      .append("\n");

    return info.toString();
  }

  /**
   * Attempts to validate that the FoundationDB library can be loaded.
   * This is useful for testing and diagnostics.
   *
   * @return true if the library was successfully configured and can be used
   */
  public static boolean validateLibrary() {
    try {
      // First try to configure the library
      if (!configureNativeLibrary()) {
        return false;
      }

      // Then try to initialize the FDB client to see if the library actually works
      com.apple.foundationdb.FDB.selectAPIVersion(740);
      return true;
    } catch (
      UnsatisfiedLinkError
      | ExceptionInInitializerError
      | NoClassDefFoundError e
    ) {
      return false;
    } catch (Exception e) {
      // Other exceptions might indicate the library loaded but FDB isn't running
      // We consider this a successful library validation
      return true;
    }
  }

  /**
   * Gets the currently configured FoundationDB library path.
   */
  public static Optional<String> getCurrentLibraryPath() {
    String path = System.getProperty("FDB_LIBRARY_PATH_FDB_C");
    return Optional.ofNullable(path);
  }
}
