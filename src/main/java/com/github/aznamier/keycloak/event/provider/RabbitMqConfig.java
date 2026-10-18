package com.github.aznamier.keycloak.event.provider;


import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.Details;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.keycloak.util.JsonSerialization;

import com.rabbitmq.client.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.regex.Pattern;


public class RabbitMqConfig {

	private static final Logger log = Logger.getLogger(RabbitMqConfig.class);
	public static final String ROUTING_KEY_PREFIX = "KK.EVENT";

	/**
	 * How events are sent to RabbitMQ: on the request thread, from a background thread, or from a background thread
	 * that keeps each event until RabbitMQ confirms it.
	 */
	public enum PublishMode {
		SYNC, ASYNC, CONFIRMED
	}
	private static final Pattern SPECIAL_CHARACTERS = Pattern.compile("[^*#a-zA-Z0-9 _.-]");
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final Pattern DOT = Pattern.compile("\\.");

	private String hosts;
	private Integer port;
	private String username;
	private String password;
	private String vhost;
	private Boolean useTls;
	// false: encrypted, but RabbitMQ's certificate and host name are not checked
	private Boolean tlsVerify;

	// SSL context settings
	private String trustStore;
	private String trustStorePass;
	private String keyStore;
	private String keyStorePass;
	//


	private String exchange;
	// Written to disk by RabbitMQ
	private Boolean persistentMessages;

	// Connection settings
	private Integer heartbeatSeconds;
	// Null keeps the client's defaults
	private Integer connectionTimeoutMs;
	private Long networkRecoveryIntervalMs;

	// Background publishing
	private PublishMode publishMode;
	private Integer publishBufferMaxMb;
	private Long shutdownFlushTimeoutMs;
	private Long confirmTimeoutMs;

	// Essential events: user event types (e.g. REGISTER) and admin events as ADMIN:<RESOURCE>.<OPERATION> (e.g. ADMIN:USER.DELETE)
	private Set<String> essentialEvents = Collections.emptySet();
	private Long essentialEventsTimeoutMs;
	
	public static String calculateRoutingKey(AdminEvent adminEvent, KeycloakSession session) {
		//KK.EVENT.ADMIN.<REALM>.<RESULT>.<RESOURCE_TYPE>.<OPERATION>
		String routingKey = ROUTING_KEY_PREFIX
				+ ".ADMIN"
				+ "." + removeDots(session.getContext().getRealm().getName())
				+ "." + (adminEvent.getError() != null ? "ERROR" : "SUCCESS")
				+ "." + adminEvent.getResourceTypeAsString()
				+ "." + adminEvent.getOperationType().toString()
				
				;
		return normalizeKey(routingKey);
	}
	
	public static String calculateRoutingKey(Event event, KeycloakSession session) {
		//KK.EVENT.CLIENT.<REALM>.<RESULT>.<CLIENT>.<EVENT_TYPE>
		String routingKey = ROUTING_KEY_PREFIX
					+ ".CLIENT"
					+ "." + removeDots(session.realms().getRealm(event.getRealmId()).getName())
					+ "." + (event.getError() != null ? "ERROR" : "SUCCESS")
					+ "." + removeDots(event.getClientId())
					+ "." + event.getType();
		
		return normalizeKey(routingKey);
	}

	//Remove all characters apart a-z, A-Z, 0-9, space, underscore, replace all spaces and hyphens with underscore
	public static String normalizeKey(CharSequence stringToNormalize) {
		return SPACE.matcher(SPECIAL_CHARACTERS.matcher(stringToNormalize).replaceAll(""))
				.replaceAll("_");
	}
	
	public static String removeDots(String stringToNormalize) {
		if(stringToNormalize != null) {
			return DOT.matcher(stringToNormalize).replaceAll("");
		}
		return stringToNormalize;
	}
	
	public static String writeAsJson(Object object, boolean isPretty) {
		try {
			if(isPretty) {
				return JsonSerialization.writeValueAsPrettyString(object);
			}
			return JsonSerialization.writeValueAsString(object);

		} catch (Exception e) {
			log.error("Could not serialize to JSON", e);
		}
		return "unparseable";
	}
	
	
	public static RabbitMqConfig createFromScope(Scope config) {
		RabbitMqConfig cfg = new RabbitMqConfig();
		
		// Required, like the vhost: a default of localhost would hide a missing or misnamed setting behind connection
		// errors. Called hosts, not url (its name before 4.0.0), because it lists host[:port] entries, not URLs
		cfg.hosts = resolveConfigVar(config, "hosts", "");
		if (cfg.hosts.trim().isEmpty()) {
			throw invalid("hosts", cfg.hosts, "set to one or more comma-separated RabbitMQ hosts, e.g. rabbitmq (it was KK_TO_RMQ_URL before 4.0.0)");
		}
		cfg.port = (int) number(config, "port", "5672", 1, 65535);
		// Fails startup on an invalid list of hosts
		cfg.getAddresses();
		cfg.username = resolveConfigVar(config, "username", "admin");
		cfg.password = resolveConfigVar(config, "password", "admin");
		// Required: RabbitMQ rejects an empty name, and a default of "/" would silently send events to the wrong
		// virtual host, where nothing may be bound to receive them, when another one was meant but not set
		cfg.vhost = resolveConfigVar(config, "vhost", "");
		if (cfg.vhost.trim().isEmpty()) {
			throw invalid("vhost", cfg.vhost, "set, e.g. to / for RabbitMQ's default virtual host");
		}
		cfg.useTls = flag(config, "use_tls", "false");
		cfg.tlsVerify = flag(config, "tls_verify", "true");

		// SSL context settings
		cfg.trustStore = resolveConfigVar(config, "trust_store", "");
		cfg.trustStorePass = resolveConfigVar(config, "trust_store_pass", "");
		cfg.keyStore = resolveConfigVar(config, "key_store", "");
		cfg.keyStorePass = resolveConfigVar(config, "key_store_pass", "");
		//

		cfg.exchange = resolveConfigVar(config, "exchange", "amq.topic");
		cfg.persistentMessages = flag(config, "persistent_messages", "false");

		// Connection settings: defaults match the RabbitMQ client's own defaults
		cfg.heartbeatSeconds = (int) number(config, "heartbeat_seconds", "60", 0, Integer.MAX_VALUE);
		String connectionTimeoutMs = resolveConfigVar(config, "connection_timeout_ms", "");
		cfg.connectionTimeoutMs = connectionTimeoutMs.trim().isEmpty() ? null
				: (int) parseNumber("connection_timeout_ms", connectionTimeoutMs, 0, Integer.MAX_VALUE);
		cfg.networkRecoveryIntervalMs = number(config, "network_recovery_interval_ms", "5000", 1, Long.MAX_VALUE);

		// Background publishing
		cfg.publishMode = publishMode(config);
		cfg.publishBufferMaxMb = (int) number(config, "publish_buffer_max_mb", "100", 1, Integer.MAX_VALUE);
		cfg.shutdownFlushTimeoutMs = number(config, "shutdown_flush_timeout_ms", "5000", 0, Long.MAX_VALUE);
		cfg.confirmTimeoutMs = number(config, "confirm_timeout_ms", "5000", 1, Long.MAX_VALUE);

		// Essential events
		cfg.setEssentialEvents(resolveConfigVar(config, "essential_events", ""));
		cfg.essentialEventsTimeoutMs = number(config, "essential_events_timeout_ms", "5000", 1, Long.MAX_VALUE);
		return cfg;
		
	}
	
	/**
	 * A whole number setting, which must be within min and max; otherwise startup fails, naming the setting.
	 */
	private static long number(Scope config, String name, String defaultValue, long min, long max) {
		return parseNumber(name, resolveConfigVar(config, name, defaultValue), min, max);
	}

	private static long parseNumber(String name, String value, long min, long max) {
		long n;
		try {
			n = Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			n = Long.MIN_VALUE;
		}
		if (n < min || n > max) {
			throw invalid(name, value, max == Long.MAX_VALUE || max == Integer.MAX_VALUE
					? "a whole number of at least " + min : "a whole number from " + min + " to " + max);
		}
		return n;
	}

	/**
	 * A true/false setting. Anything else fails startup, rather than silently meaning false.
	 */
	private static boolean flag(Scope config, String name, String defaultValue) {
		String value = resolveConfigVar(config, name, defaultValue);
		if ("true".equalsIgnoreCase(value.trim())) {
			return true;
		}
		if ("false".equalsIgnoreCase(value.trim())) {
			return false;
		}
		throw invalid(name, value, "true or false");
	}

	private static PublishMode publishMode(Scope config) {
		String value = resolveConfigVar(config, "publish_mode", "sync");
		for (PublishMode mode : PublishMode.values()) {
			if (mode.name().equalsIgnoreCase(value.trim())) {
				return mode;
			}
		}
		throw invalid("publish_mode", value, "one of sync, async, confirmed");
	}

	private static IllegalArgumentException invalid(String name, String value, String expected) {
		return new IllegalArgumentException(String.format("keycloak-to-rabbitmq setting %s (KK_TO_RMQ_%s) must be %s, got '%s'",
				name, name.toUpperCase(Locale.ENGLISH), expected, value));
	}

	private static String resolveConfigVar(Scope config, String variableName, String defaultValue) {
		
		String value = defaultValue;
		// Keycloak options are usually hyphenated (e.g. --spi-events-listener-keycloak-to-rabbitmq-heartbeat-seconds)
		String fromKeycloak = config == null ? null
				: config.get(variableName) != null ? config.get(variableName) : config.get(variableName.replace('_', '-'));
		if (fromKeycloak != null) {
			value = fromKeycloak;
		} else {
			//try from env variables eg: KK_TO_RMQ_HOSTS:
			String envVariableName = "KK_TO_RMQ_" + variableName.toUpperCase(Locale.ENGLISH);
			String env = System.getenv(envVariableName);
			if(env != null) {
				value = env;
			}
		}
		// Secrets (password, key_store_pass, trust_store_pass) only show whether they are set
		String logged = variableName.contains("pass") && !value.isEmpty() ? "<hidden>" : value;
		log.infof("keycloak-to-rabbitmq configuration: %s=%s%n", variableName, logged);
		return value;
		
	}
	
	
	/**
	 * The RabbitMQ hosts to connect to: the url setting lists one or more, comma-separated, each optionally with
	 * :port (otherwise the port setting). The client tries them in turn when connecting and recovering.
	 */
	public List<Address> getAddresses() {
		List<Address> addresses = new ArrayList<>();
		for (String entry : hosts.split(",")) {
			if (entry.trim().isEmpty()) {
				continue;
			}
			Address address;
			try {
				address = Address.parseAddress(entry.trim());
			} catch (RuntimeException e) {
				throw invalidHosts();
			}
			if (address.getPort() == -1) {
				address = new Address(address.getHost(), port);
			} else if (address.getPort() < 1 || address.getPort() > 65535) {
				throw invalidHosts();
			}
			addresses.add(address);
		}
		if (addresses.isEmpty()) {
			throw invalidHosts();
		}
		return addresses;
	}

	private IllegalArgumentException invalidHosts() {
		return invalid("hosts", hosts, "one or more comma-separated hosts, each optionally with :port (IPv6 addresses in brackets)");
	}

	/** The hosts, for log messages: e.g. "rabbit-1:5672, rabbit-2:5672". */
	public String describeAddresses() {
		return getAddresses().stream().map(a -> a.getHost() + ":" + a.getPort()).collect(Collectors.joining(", "));
	}

	public String getHosts() {
		return hosts;
	}
	public void setHosts(String hosts) {
		this.hosts = hosts;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getVhost() {
		return vhost;
	}
	public void setVhost(String vhost) {
		this.vhost = vhost;
	}
	public Boolean getUseTls() {
		return useTls;
	}
	public void setUseTls(Boolean useTls) {
		this.useTls = useTls;
	}
	public Boolean getTlsVerify() {
		return tlsVerify;
	}
	public void setTlsVerify(Boolean tlsVerify) {
		this.tlsVerify = tlsVerify;
	}

	// setters and getters SSL context setting
	public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}
	public void setTrustStorePass(String trustStorePass) {
		this.trustStorePass = trustStorePass;
	}
	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}
	public void setKeyStorePass(String keyStorePass) {
		this.keyStorePass = keyStorePass;
	}
	public String getTrustStore() {
		return trustStore;
	}
	public String getTrustStorePass() {
		return trustStorePass;
	}
	public String getKeyStore() {
		return keyStore;
	}
	public String getKeytStorePass() {
		return keyStorePass;
	}
	//

	public String getExchange() {
		return exchange;
	}
	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public Boolean getPersistentMessages() {
		return persistentMessages;
	}
	public void setPersistentMessages(Boolean persistentMessages) {
		this.persistentMessages = persistentMessages;
	}

	public Integer getHeartbeatSeconds() {
		return heartbeatSeconds;
	}
	public void setHeartbeatSeconds(Integer heartbeatSeconds) {
		this.heartbeatSeconds = heartbeatSeconds;
	}
	public Integer getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}
	public void setConnectionTimeoutMs(Integer connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}
	public Long getNetworkRecoveryIntervalMs() {
		return networkRecoveryIntervalMs;
	}
	public void setNetworkRecoveryIntervalMs(Long networkRecoveryIntervalMs) {
		this.networkRecoveryIntervalMs = networkRecoveryIntervalMs;
	}

	public PublishMode getPublishMode() {
		return publishMode;
	}
	public void setPublishMode(PublishMode publishMode) {
		this.publishMode = publishMode;
	}
	public Integer getPublishBufferMaxMb() {
		return publishBufferMaxMb;
	}
	public void setPublishBufferMaxMb(Integer publishBufferMaxMb) {
		this.publishBufferMaxMb = publishBufferMaxMb;
	}
	public Long getShutdownFlushTimeoutMs() {
		return shutdownFlushTimeoutMs;
	}
	public void setShutdownFlushTimeoutMs(Long shutdownFlushTimeoutMs) {
		this.shutdownFlushTimeoutMs = shutdownFlushTimeoutMs;
	}

	public Long getConfirmTimeoutMs() {
		return confirmTimeoutMs;
	}
	public void setConfirmTimeoutMs(Long confirmTimeoutMs) {
		this.confirmTimeoutMs = confirmTimeoutMs;
	}
	public Set<String> getEssentialEvents() {
		return essentialEvents;
	}
	/**
	 * @param essentialEvents comma-separated list, e.g. "REGISTER,ADMIN:USER.DELETE"
	 */
	public void setEssentialEvents(String essentialEvents) {
		this.essentialEvents = parseEventNames(essentialEvents);
	}
	private static Set<String> parseEventNames(String names) {
		Set<String> parsed = new LinkedHashSet<>();
		if (names != null) {
			for (String name : names.split(",")) {
				if (!name.trim().isEmpty()) {
					parsed.add(name.trim().toUpperCase(Locale.ROOT));
				}
			}
		}
		return Collections.unmodifiableSet(parsed);
	}
	public boolean isEssential(Event event) {
		return !essentialEvents.isEmpty() && event.getError() == null && event.getType() != null
				&& essentialEvents.contains(event.getType().name()) && !isBrokerRegistration(event);
	}

	/**
	 * A REGISTER from a first login through an identity provider is emitted one request after Keycloak created
	 * and committed the user, so failing that request cannot undo the sign-up; it would only leave the user
	 * unable to finish it. Such events are published like any other instead.
	 * (IDENTITY_PROVIDER_FIRST_LOGIN is emitted in the request that creates the user, so it can be enforced.)
	 */
	private static boolean isBrokerRegistration(Event event) {
		return event.getType() == EventType.REGISTER && event.getDetails() != null
				&& "broker".equals(event.getDetails().get(Details.REGISTER_METHOD));
	}
	public boolean isEssential(AdminEvent adminEvent) {
		return !essentialEvents.isEmpty() && adminEvent.getError() == null && adminEvent.getOperationType() != null
				&& essentialEvents.contains(adminEventName(adminEvent));
	}
	static String adminEventName(AdminEvent adminEvent) {
		return ("ADMIN:" + adminEvent.getResourceTypeAsString() + "." + adminEvent.getOperationType()).toUpperCase(Locale.ROOT);
	}
	public Long getEssentialEventsTimeoutMs() {
		return essentialEventsTimeoutMs;
	}
	public void setEssentialEventsTimeoutMs(Long essentialEventsTimeoutMs) {
		this.essentialEventsTimeoutMs = essentialEventsTimeoutMs;
	}

}
