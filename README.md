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

###### TLS
With `KK_TO_RMQ_USE_TLS=true` the connection uses TLS 1.3 or 1.2, whichever both sides support, and checks RabbitMQ's certificate:
  - against `KK_TO_RMQ_TRUST_STORE` (a JKS file, with `KK_TO_RMQ_TRUST_STORE_PASS`) if set, otherwise against the JVM's trusted certificates
  - the certificate must match the host in `KK_TO_RMQ_URL`
  - `KK_TO_RMQ_KEY_STORE` (a PKCS12 file, with `KK_TO_RMQ_KEY_STORE_PASS`) adds a client certificate, for RabbitMQ setups that require one
  - if TLS can't be set up (e.g. a missing or unreadable truststore), Keycloak doesn't start, rather than connecting without TLS

###### Connection
The connection to RabbitMQ is opened in the background when Keycloak starts and kept open, so requests never wait on it.
If RabbitMQ is unreachable, Keycloak still starts and the connection is retried; after a connection drops, the RabbitMQ
client's automatic recovery reconnects it. Events emitted while there is no connection are logged as not sent.
