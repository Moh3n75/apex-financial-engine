package com.apex.cell;

import org.testcontainers.DockerClientFactory;

/**
 * Shared gate for optional Testcontainers / Docker checks.
 */
public final class DockerAvailability {

    private DockerAvailability() {
    }

    public static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable ignored) {
            return false;
        }
    }
}
