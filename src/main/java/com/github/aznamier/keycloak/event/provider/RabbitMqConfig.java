package com.github.aznamier.keycloak.event.provider;


import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.keycloak.util.JsonSerialization;

import java.util.Locale;
import java.util.regex.Pattern;


public class RabbitMqConfig {

	private static final Logger log = Logger.getLogger(RabbitMqConfig.class);
	public static final String ROUTING_KEY_PREFIX = "KK.EVENT";
	private static final Pattern SPECIAL_CHARACTERS = Pattern.compile("[^*#a-zA-Z0-9 _.-]");
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final Pattern DOT = Pattern.compile("\\.");

	private String hostUrl;
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

	// Connection settings
	private Integer heartbeatSeconds;
	// Null keeps the client's defaults
	private Integer connectionTimeoutMs;
	private Long networkRecoveryIntervalMs;
	
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
		
		cfg.hostUrl = resolveConfigVar(config, "url", "localhost");
		cfg.port = (int) number(config, "port", "5672", 1, 65535);
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

		// Connection settings: defaults match the RabbitMQ client's own defaults
		cfg.heartbeatSeconds = (int) number(config, "heartbeat_seconds", "60", 0, Integer.MAX_VALUE);
		String connectionTimeoutMs = resolveConfigVar(config, "connection_timeout_ms", "");
		cfg.connectionTimeoutMs = connectionTimeoutMs.trim().isEmpty() ? null
				: (int) parseNumber("connection_timeout_ms", connectionTimeoutMs, 0, Integer.MAX_VALUE);
		cfg.networkRecoveryIntervalMs = number(config, "network_recovery_interval_ms", "5000", 1, Long.MAX_VALUE);
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

	private static IllegalArgumentException invalid(String name, String value, String expected) {
		return new IllegalArgumentException(String.format("keycloak-to-rabbitmq setting %s (KK_TO_RMQ_%s) must be %s, got '%s'",
				name, name.toUpperCase(Locale.ENGLISH), expected, value));
	}

	private static String resolveConfigVar(Scope config, String variableName, String defaultValue) {
		
		String value = defaultValue;
		if(config != null && config.get(variableName) != null) {
			value = config.get(variableName);
		} else {
			//try from env variables eg: KK_TO_RMQ_URL:
			String envVariableName = "KK_TO_RMQ_" + variableName.toUpperCase(Locale.ENGLISH);
			String env = System.getenv(envVariableName);
			if(env != null) {
				value = env;
			}
		}
		if (!"password".equals(variableName)) {
			log.infof("keycloak-to-rabbitmq configuration: %s=%s%n", variableName, value);
		}
		return value;
		
	}
	
	
	public String getHostUrl() {
		return hostUrl;
	}
	public void setHostUrl(String hostUrl) {
		this.hostUrl = hostUrl;
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

}
