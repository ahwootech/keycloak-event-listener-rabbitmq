package com.github.aznamier.keycloak.event.provider;

import com.rabbitmq.client.ConnectionFactory;
//
import java.io.*;
import java.security.*;
import javax.net.ssl.*;
//
import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class RabbitMqEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger log = Logger.getLogger(RabbitMqEventListenerProviderFactory.class);
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
            try {
                Boolean context = false;
                SSLContext c = SSLContext.getInstance("TLSv1.2");

                TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
                if (! cfg.getTrustStore().isEmpty()){        
                    char[] trustPassphrase = cfg.getTrustStorePass().toCharArray();
                    KeyStore tks = KeyStore.getInstance("JKS");
                    tks.load(new FileInputStream(cfg.getTrustStore()), trustPassphrase);
            
                    tmf.init(tks);

                    c.init(null, tmf.getTrustManagers(), null);
                    context = true;
                }               
                
                KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                if (! cfg.getKeyStore().isEmpty()){
                    char[] keyPassphrase = cfg.getKeytStorePass().toCharArray();
                    KeyStore ks = KeyStore.getInstance("PKCS12");
                    ks.load(new FileInputStream(cfg.getKeyStore()), keyPassphrase);
                    
                    kmf.init(ks, keyPassphrase);
                    
                    if (context){
                        c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                    }
                    else{
                        c.init(kmf.getKeyManagers(), null, null);
                        context = true;
                    }
                }

                if ( context ){
                    this.connectionFactory.useSslProtocol(c);
                }
                else {
                    this.connectionFactory.useSslProtocol();
                }
                
            }
            catch (Exception e) {
                log.error("Could not use SSL protocol", e);
            }
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
