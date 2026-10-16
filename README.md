# keycloak-event-listener-rabbitmq

##### A Keycloak SPI plugin that publishes events to a RabbitMq server.  

| Plugin | min Keycloak ver |
| -- | ---- |
| 1.x | 10.x |
| 2.x | 13.x |
| 3.x | 16.x |
| 4.x | 26.x |

For example here is the notification of the user updated by administrator

* routing key: `KK.EVENT.ADMIN.MYREALM.SUCCESS.USER.UPDATE`  
* published to exchange: `amq.topic`
* content: 


```
{
  "@class" : "com.github.aznamier.keycloak.event.provider.EventAdminNotificationMqMsg",
  "time" : 1596951200408,
  "realmId" : "MYREALM",
  "authDetails" : {
    "realmId" : "master",
    "clientId" : "********-****-****-****-**********",
    "userId" : "********-****-****-****-**********",
    "ipAddress" : "192.168.1.1"
  },
  "resourceType" : "USER",
  "operationType" : "UPDATE",
  "resourcePath" : "users/********-****-****-****-**********",
  "representation" : "representation details here....",
  "error" : null,
  "resourceTypeAsString" : "USER"
}
```

The routing key is calculated as follows:
* admin events: `KK.EVENT.ADMIN.<REALM>.<RESULT>.<RESOURCE_TYPE>.<OPERATION>`
* client events: `KK.EVENT.CLIENT.<REALM>.<RESULT>.<CLIENT>.<EVENT_TYPE>`

And because the recommended exchange is a **TOPIC (amq.topic)**,  
therefore its easy for Rabbit client to subscribe to selective combinations eg:
* all events: `KK.EVENT.#`
* all events from my realm: `KK.EVENT.*.MYREALM.#`
* all error events from my realm: `KK.EVENT.*.MYREALM.ERROR.#`
* all user events from my-relam and my-client: `KK.EVENT.*.MY-REALM.*.MY-CLIENT.USER`


## USAGE:
1. [Download the latest jar](https://github.com/aznamier/keycloak-event-listener-rabbitmq/blob/target/keycloak-to-rabbit-4.0.0.jar?raw=true) or build from source (Java 17+): ``mvn clean install``
2. Copy jar into your Keycloak: `/opt/keycloak/providers/keycloak-to-rabbit-4.0.0.jar`
3. Configure as described below
4. Restart the Keycloak server
5. Enable logging in Keycloak UI by adding **keycloak-to-rabbitmq**  
 `Manage > Events > Config > Events Config > Event Listeners`

#### Configuration 
###### Environment variables
An invalid value, such as a number with extra characters (`5672x`), a number out of range, or a true/false setting set to anything other than `true` or `false`, stops Keycloak from starting with an error naming the setting.
  - `KK_TO_RMQ_URL` - default: *localhost*
  - `KK_TO_RMQ_PORT` - default: *5672*
  - `KK_TO_RMQ_VHOST` - default: *empty*
  - `KK_TO_RMQ_EXCHANGE` - default: *amq.topic*
  - `KK_TO_RMQ_USERNAME` - default: *admin*
  - `KK_TO_RMQ_PASSWORD` - default: *admin*
  - `KK_TO_RMQ_USE_TLS` - default: *false*
  - `KK_TO_RMQ_KEY_STORE` - default: *empty*
  - `KK_TO_RMQ_KEY_STORE_PASS` - default: *empty*
  - `KK_TO_RMQ_TRUST_STORE` - default: *empty*
  - `KK_TO_RMQ_TRUST_STORE_PASS` - default: *empty*
  - `KK_TO_RMQ_TLS_VERIFY` - default: *true*
  - `KK_TO_RMQ_HEARTBEAT_SECONDS` - default: *60*
  - `KK_TO_RMQ_CONNECTION_TIMEOUT_MS` - default: *empty* (the client's 60000 for the TCP connection and 10000 for the handshake)
  - `KK_TO_RMQ_NETWORK_RECOVERY_INTERVAL_MS` - default: *5000*

###### TLS
With `KK_TO_RMQ_USE_TLS=true` the connection uses TLS 1.3 or 1.2, whichever both sides support, and checks RabbitMQ's certificate:
  - against `KK_TO_RMQ_TRUST_STORE` (a JKS file, with `KK_TO_RMQ_TRUST_STORE_PASS`) if set, otherwise against the JVM's trusted certificates
  - the certificate must match the host in `KK_TO_RMQ_URL`
  - `KK_TO_RMQ_KEY_STORE` (a PKCS12 file, with `KK_TO_RMQ_KEY_STORE_PASS`) adds a client certificate, for RabbitMQ setups that require one
  - if TLS can't be set up (e.g. a missing or unreadable truststore), Keycloak doesn't start, rather than connecting without TLS
  - `KK_TO_RMQ_TLS_VERIFY=false` skips the certificate and host name checks: the connection is still encrypted, but anyone who can intercept it could impersonate RabbitMQ, and a warning is logged at startup. It's meant for a self-signed certificate on a private network; adding that certificate to a truststore is the safer option

###### Connection
The connection to RabbitMQ is opened in the background when Keycloak starts and kept open, so connecting never holds up a
request. If RabbitMQ is unreachable, Keycloak still starts and the connection is retried; after a connection drops, the
RabbitMQ client's automatic recovery reconnects it. Events emitted while there is no connection are logged as not sent.
Events are still sent on the request thread, so while RabbitMQ is connected but not responding (e.g. frozen, or blocking
publishers when it is low on memory or disk), or until a host that died with the connection open is noticed, requests
wait on it. The defaults below are the RabbitMQ Java client's own:
  - `KK_TO_RMQ_HEARTBEAT_SECONDS` - how often the client and RabbitMQ check the other is still there. A connection to a broker that disappears without closing it (e.g. a host failure) is only detected after about two missed heartbeats, so lower values detect it sooner, e.g. *10*. RabbitMQ uses the lower of the client's and the server's values
  - `KK_TO_RMQ_CONNECTION_TIMEOUT_MS` - how long a connection attempt waits for the TCP connection, and again for RabbitMQ to respond once it is open (e.g. when the broker is frozen or behind a proxy). Against an unreachable or frozen broker every attempt waits this long, so lower values reconnect sooner once RabbitMQ is back, e.g. *5000*. When not set, the client's defaults are used: 60000 for the TCP connection and 10000 for the response
  - `KK_TO_RMQ_NETWORK_RECOVERY_INTERVAL_MS` - how long to wait between reconnect attempts, both for the first connection at startup and after a connection drops
