package com.github.aznamier.keycloak.event.provider;

import com.rabbitmq.client.ConnectionFactory;
//
import java.io.*;
import java.security.*;
import javax.net.ssl.*;
//
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class RabbitMqEventListenerProviderFactory implements EventListenerProviderFactory {

    private RabbitMqConfig cfg;
    private ConnectionFactory connectionFactory;
    private RabbitMqConnectionManager connectionManager;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new RabbitMqEventListenerProvider(connectionManager, session, cfg);
    }

    @Override
    public void init(Scope config) {
        cfg = RabbitMqConfig.createFromScope(config);
        this.connectionFactory = new ConnectionFactory();

        this.connectionFactory.setUsername(cfg.getUsername());
        this.connectionFactory.setPassword(cfg.getPassword());
        this.connectionFactory.setVirtualHost(cfg.getVhost());
        this.connectionFactory.setHost(cfg.getHostUrl());
        this.connectionFactory.setPort(cfg.getPort());
        this.connectionFactory.setAutomaticRecoveryEnabled(true);

        if (cfg.getUseTls()) {
            useTls();
        }
    }

    /**
     * TLS with the highest version both sides support, verifying RabbitMQ's certificate (against the truststore if
     * one is configured, otherwise the JVM's trusted certificates) and that it matches the host. A client certificate
     * is presented if a keystore is configured. Fails Keycloak's startup rather than connecting without TLS.
     */
    private void useTls() {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            if (cfg.getTrustStore().isEmpty()) {
                tmf.init((KeyStore) null);
            } else {
                KeyStore trustStore = KeyStore.getInstance("JKS");
                try (InputStream in = new FileInputStream(cfg.getTrustStore())) {
                    trustStore.load(in, cfg.getTrustStorePass().toCharArray());
                }
                tmf.init(trustStore);
            }

            KeyManager[] keyManagers = null;
            if (!cfg.getKeyStore().isEmpty()) {
                char[] keyPassphrase = cfg.getKeytStorePass().toCharArray();
                KeyStore keyStore = KeyStore.getInstance("PKCS12");
                try (InputStream in = new FileInputStream(cfg.getKeyStore())) {
                    keyStore.load(in, keyPassphrase);
                }
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, keyPassphrase);
                keyManagers = kmf.getKeyManagers();
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, tmf.getTrustManagers(), null);
            this.connectionFactory.useSslProtocol(sslContext);
            this.connectionFactory.enableHostnameVerification();
        } catch (Exception e) {
            throw new IllegalStateException("keycloak-to-rabbitmq could not set up TLS for RabbitMQ: " + e.getMessage(), e);
        }
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        this.connectionManager = new RabbitMqConnectionManager(connectionFactory, cfg);
        this.connectionManager.start();
    }

    @Override
    public void close() {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Override
    public String getId() {
        return "keycloak-to-rabbitmq";
    }

}
