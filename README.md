# SmipMqttService

Helper service for the SMIP Gateway.

This service caches MQTT topics and messages and stores them to be read by the SMIP Gateway at the next update.

Because MQTT topics and messages are very free-form (that is, not type safe) all simple values are treated as strings,
and topics and their payloads can be parsed as individual "tags" in the SMIP. This parsing can be controlled with the **appsettings.json** file.

## Parsing Tags

### Hierarchical Topics as Tags

The `topicSeperator` property in the `Mqtt` section of **appsettings.json** indicates how a compound topic name should be parsed into multiple topics.

The default seperator is `/` such that a topic like: `parent/child/grandchild` will be parsed into three seperate tags:
- `parent`
- `parent/child`
- `parent/child/grandparent`

This behavior is seen in other MQTT clients, including MQTT Explorer. It can be disabled by setting the `topicSeperator` value to an empty string (or removing the property entirely) in **appsettings.json**.

### Payload JSON Members as Tags

The `virtualTopicSeperator` property in the `Mqtt` section of **appsettings.json** indicates that the service should attempt parse JSON payloads of MQTT messages into seperate tags.
If present, the `virtualTopicSeperator` will be added between the real topic name, and the parsed JSON member(s).

The default virtual topic seperator is `/:/`, and is used in combination with the `topicSeperator`. For example, if a MQTT message with the topic `parent` has a parseable JSON body like:

```
{
child: {
	name: "Jonathan"
}
```

Real and Virtual Topics will be created like:
- `parent`
- `parent/:/child`
- `parent/:/child/name`

This behavior is not seen elsewhere, but aids in mapping complex MQTT messages to an information model. It can be disabled by setting the `virtualTopicSeperator` value to an empty string (or removing the property entirely) in **appsettings.json**.
