package nl.kii.eventstore

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.TextNode
import io.vertx.core.json.Json
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.util.Map
import nl.kii.eventsourcing.AggregateRoot
import nl.kii.eventsourcing.DomainEventMessage
import nl.kii.eventsourcing.GenericDomainEventMessage
import org.apache.kafka.common.serialization.Deserializer

import static extension java.lang.Class.forName
import static extension java.time.Instant.ofEpochSecond
import static extension java.util.UUID.fromString
import nl.kii.eventsourcing.Event
import java.io.Serializable

class DomainEventMessageJsonDeserializer extends JsonDeserializer<DomainEventMessage<? extends Event>> {

    override deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        val node = p.codec.readTree(p)

        val id = (node.get('id') as TextNode).asText.fromString
        val aggregateId = (node.get('aggregateId') as TextNode).asText.fromString
        val aggregateType = (node.get('aggregateType') as TextNode).asText
        val timestamp = (node.get('timestamp') as NumericNode).asLong.ofEpochSecond

        val payloadType = (node.get('payloadType') as TextNode).asText.forName
        val payload = Json.mapper.treeToValue(node.get('payload'), payloadType)

        val metadata = Json.mapper.treeToValue(node.get('metadata'), Map)

        new GenericDomainEventMessage(id, timestamp, aggregateId, aggregateType, payload, metadata)
    }

}

class DomainEventMessageDeserializer implements Deserializer<DomainEventMessage<? extends Event>> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserialize(String topic, byte[] data) {
        Json.mapper.readValue(data, GenericDomainEventMessage)
    }

}

class AggregateRootDeserializer implements Deserializer<AggregateRoot> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserialize(String topic, byte[] data) {
        val bais = new ByteArrayInputStream(data)
        val in = new ObjectInputStream(bais)
        in.readObject as AggregateRoot => [
            in.close
            bais.close
        ]
    }

}

class JavaDeserializer<T extends Serializable> implements Deserializer<T> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserialize(String topic, byte[] data) {
        val bais = new ByteArrayInputStream(data)
        val in = new ObjectInputStream(bais)
        in.readObject as T => [
            in.close
            bais.close
        ]
    }

}