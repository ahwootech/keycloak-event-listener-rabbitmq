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
  "id" : "********-****-****-****-**********",
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
Each setting can also be given as a Keycloak option, which takes precedence: `KK_TO_RMQ_HEARTBEAT_SECONDS` as
`--spi-events-listener-keycloak-to-rabbitmq-heartbeat-seconds` (or the `KC_SPI_EVENTS_LISTENER_KEYCLOAK_TO_RABBITMQ_HEARTBEAT_SECONDS` environment variable), and so on.
An invalid value, such as a number with extra characters (`5672x`), a number out of range, or a true/false setting set to anything other than `true` or `false`, stops Keycloak from starting with an error naming the setting.
A `KK_TO_RMQ_*` environment variable that isn't one of the settings below, e.g. a misspelled name, is logged as a warning at startup and otherwise ignored.
  - `KK_TO_RMQ_URL` - default: *localhost*. One or more RabbitMQ hosts, comma-separated, each optionally with `:port` (otherwise `KK_TO_RMQ_PORT`; IPv6 addresses in brackets), e.g. `rabbit-1,rabbit-2,rabbit-3:5673`. The connection and its automatic recovery try them in turn
  - `KK_TO_RMQ_PORT` - default: *5672*
  - `KK_TO_RMQ_VHOST` - required, e.g. */* for RabbitMQ's default virtual host
  - `KK_TO_RMQ_EXCHANGE` - default: *amq.topic*
  - `KK_TO_RMQ_PERSISTENT_MESSAGES` - default: *false*
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
  - `KK_TO_RMQ_PUBLISH_MODE` - default: *sync*
  - `KK_TO_RMQ_PUBLISH_BUFFER_MAX_MB` - default: *100*
  - `KK_TO_RMQ_SHUTDOWN_FLUSH_TIMEOUT_MS` - default: *5000*
  - `KK_TO_RMQ_CONFIRM_TIMEOUT_MS` - default: *5000*
  - `KK_TO_RMQ_ESSENTIAL_EVENTS` - default: *empty*
  - `KK_TO_RMQ_ESSENTIAL_EVENTS_TIMEOUT_MS` - default: *5000*
  - `KK_TO_RMQ_EXCLUDED_EVENTS` - default: *empty*

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
RabbitMQ client's automatic recovery reconnects it. By default, events emitted while there is no connection are logged as
not sent, and events are sent on the request thread, so while RabbitMQ is connected but not responding (e.g. frozen, or
blocking publishers when it is low on memory or disk), or until a host that died with the connection open is noticed,
requests wait on it; [background publishing](#background-publishing) avoids both. The defaults below are the RabbitMQ
Java client's own:
  - `KK_TO_RMQ_HEARTBEAT_SECONDS` - how often the client and RabbitMQ check the other is still there. A connection to a broker that disappears without closing it (e.g. a host failure) is only detected after about two missed heartbeats, so lower values detect it sooner, e.g. *10*. RabbitMQ uses the lower of the client's and the server's values
  - `KK_TO_RMQ_CONNECTION_TIMEOUT_MS` - how long a connection attempt waits for the TCP connection, and again for RabbitMQ to respond once it is open (e.g. when the broker is frozen or behind a proxy). Against an unreachable or frozen broker every attempt waits this long, so lower values reconnect sooner once RabbitMQ is back, e.g. *5000*. When not set, the client's defaults are used: 60000 for the TCP connection and 10000 for the response
  - `KK_TO_RMQ_NETWORK_RECOVERY_INTERVAL_MS` - how long to wait between reconnect attempts, both for the first connection at startup and after a connection drops

Connection problems are logged at ERROR level with the prefix `keycloak-to-rabbitmq CONNECTION PROBLEM` as soon as
they happen, whether or not any events are being sent, so they can be used for alerting:
  - the connection being lost, with the reason (e.g. missed heartbeats, or RabbitMQ closing it)
  - failing to connect at startup, with the error; while the connection stays down, a summary at most once a minute with how long it has been down and how many reconnect attempts failed
  - RabbitMQ blocking publishers (e.g. when it is low on memory or disk), with its reason. RabbitMQ only tells a connection it is blocked when it publishes during the alarm, so this is logged at the first event sent while it lasts
  - connecting or reconnecting (`reconnected to RabbitMQ at <host>:<port> after <duration> and <n> failed attempts`) and RabbitMQ unblocking publishers are logged at INFO

The RabbitMQ client's own warning for each connection error (`An unexpected connection driver error occurred`) repeats what these lines report, so it is logged at DEBUG instead.

###### Background publishing
`KK_TO_RMQ_PUBLISH_MODE` sets how events are sent:
  - `sync` (the default) - on the Keycloak request thread, once the request's transaction completes
  - `async` - from a single background thread, so requests such as logins never wait on RabbitMQ
  - `confirmed` - like `async`, and each event is kept until RabbitMQ confirms it has it (see [publisher confirms](#publisher-confirms))

With `async` or `confirmed`:
  - while RabbitMQ is unreachable or not accepting messages, events wait in an in-memory buffer and are sent in order once it is back
  - `KK_TO_RMQ_PUBLISH_BUFFER_MAX_MB` limits the buffer's memory (each buffered event counts as about 1.4 KB, so 100 MB holds roughly 75,000 events; size it as event rate × the longest outage to ride out × 1.4 KB); when it is full, new events are dropped. A warning is logged when it passes half full
  - on shutdown, buffered events are sent for up to `KK_TO_RMQ_SHUTDOWN_FLUSH_TIMEOUT_MS`
  - buffered events are lost if Keycloak stops or crashes before they are sent
  - with `async`, events sent just before a connection drops, before the client notices, can be lost without an error

Every buffered event that is never sent is logged at ERROR level with the prefix `keycloak-to-rabbitmq PUBLISH FAILED`,
which can be used for alerting. Dropped events are reported at most once a minute, with a count.

###### Publisher confirms
With `KK_TO_RMQ_PUBLISH_MODE=confirmed`, events are sent in batches and only removed from the buffer once RabbitMQ
confirms it has them, so events sent into a connection that has just died are sent again once it is back instead of
being lost:
  - a batch that RabbitMQ rejects, or doesn't confirm within `KK_TO_RMQ_CONFIRM_TIMEOUT_MS`, is sent again, one event at a time, so events RabbitMQ accepts aren't held up by one it keeps rejecting
  - consumers may therefore receive an event more than once
  - RabbitMQ confirms an event once it has routed it, or found no queue to route it to; an event no queue is bound for is confirmed and discarded

###### Messages
Every message's `messageId` property is Keycloak's own ID for the event, which is also in the message body as `id`, so consumers can detect duplicates and find the event in Keycloak's stored events.
When Keycloak tracing is enabled, every message also carries the trace of the request that produced the event, in the `traceparent` header (W3C format, so OpenTelemetry consumers can continue the trace) and as a plain `traceId` header.
  - `KK_TO_RMQ_PERSISTENT_MESSAGES=true` - messages are sent as persistent, so RabbitMQ writes them to disk and they survive a broker restart. Persistence is set on each message by the publisher, not by the queue: a durable queue survives a restart, but a classic queue drops the transient messages in it. It only helps for durable queues; quorum queues and streams persist every message whether or not this is set
  - with `KK_TO_RMQ_PUBLISH_MODE=confirmed` too, an event RabbitMQ has confirmed survives a broker restart or crash, as long as the queue it was routed to is durable

###### Essential events
Some events must not be lost, e.g. a `REGISTER` that another system builds the user's profile from.
`KK_TO_RMQ_ESSENTIAL_EVENTS` lists them, comma-separated: user event types (e.g. `REGISTER,UPDATE_EMAIL`) and admin events as
`ADMIN:<RESOURCE_TYPE>.<OPERATION>` (e.g. `ADMIN:USER.DELETE`). Only successful events are treated as essential.
A name that isn't a Keycloak event type or admin operation stops Keycloak from starting with an error naming it, as it would never match; an unknown admin resource type is only a warning, as it may come from an extension.

An essential event is sent before Keycloak commits the request that produced it, and the request waits until RabbitMQ confirms it:
  - if RabbitMQ does not confirm it within `KK_TO_RMQ_ESSENTIAL_EVENTS_TIMEOUT_MS`, rejects it, no queue is bound to receive it, or RabbitMQ is not connected, the request is rolled back and the user gets an error and can try again. This is logged with the `keycloak-to-rabbitmq PUBLISH FAILED` prefix
  - essential events are sent on their own channel, are always persistent, and carry Keycloak's event ID as their `messageId`
  - make sure a queue is bound for every essential event before enabling it, otherwise every such action fails
  - consumers may rarely receive an essential event for an action that was rolled back (e.g. if the commit itself fails after RabbitMQ confirmed the event), so they should check the current state, e.g. that the user exists
  - some Keycloak flows commit a change in an earlier request than the one that emits its event, so failing that request cannot undo the change, and consumers should still handle a missing event for those:
    - a first login through an identity provider creates the user one request before `REGISTER` (with `register_method=broker`) is emitted. Failing it would leave the user unable to finish signing up, so such a `REGISTER` is never treated as essential and is published like any other event. To make these sign-ups fail cleanly while RabbitMQ is unavailable, add `IDENTITY_PROVIDER_FIRST_LOGIN`, which is emitted in the request that creates the user
    - linking an identity provider through the `idp_link` application initiated action links it one request before `FEDERATED_IDENTITY_LINK` is emitted

###### Excluded events
`KK_TO_RMQ_EXCLUDED_EVENTS` lists events that are never published, using the same syntax and name check as essential events
(e.g. `REFRESH_TOKEN,CODE_TO_TOKEN`). Very frequent token events can make up most of Keycloak's events, so excluding the
ones no consumer needs reduces the load on Keycloak, RabbitMQ and consumers:
  - excluded events are skipped before anything else is done with them
  - excluding them also slows how fast the publish buffer fills while RabbitMQ is unreachable
  - error events have their own type, so excluding `REFRESH_TOKEN` still publishes `REFRESH_TOKEN_ERROR`
  - an event can't be both essential and excluded: listing it in both stops Keycloak from starting with an error naming it
