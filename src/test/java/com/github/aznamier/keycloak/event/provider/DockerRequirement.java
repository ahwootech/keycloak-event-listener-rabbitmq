package com.github.aznamier.keycloak.event.provider;

import org.testcontainers.DockerClientFactory;

/**
 * Decides whether the integration tests run. Without Docker they are skipped, so the build still works on a
 * machine without it, unless -Dit.requireDocker=true is set, as CI does: then they run and fail, so CI can't
 * pass without them.
 */
final class DockerRequirement {

	static final String CONDITION = "com.github.aznamier.keycloak.event.provider.DockerRequirement#dockerAvailableOrRequired";

	private DockerRequirement() {
	}

	static boolean dockerAvailableOrRequired() {
		return Boolean.getBoolean("it.requireDocker") || DockerClientFactory.instance().isDockerAvailable();
	}
}
