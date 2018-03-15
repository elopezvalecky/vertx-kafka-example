package nl.kii.eventstore

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import io.vertx.core.json.Json
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectOutputStream
import java.io.Serializable
import java.util.Map
import nl.kii.eventsourcing.AggregateRoot
import nl.kii.eventsourcing.DomainEventMessage
import nl.kii.eventsourcing.Event
import org.apache.kafka.common.serialization.Serializer

class DomainEventMessageJsonSerializer extends JsonSerializer<DomainEventMessage<? extends Event>> {

    override serialize(DomainEventMessage<? extends Event> value, JsonGenerator gen,
        SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStartObject
        gen.writeObjectField('id', value.id)
        gen.writeObjectField('aggregateId', value.aggregateId)
        gen.writeStringField('aggregateType', value.aggregateType)
        gen.writeNumberField('timestamp', value.timestamp.epochSecond)
        gen.writeObjectField('payload', value.payload)
        gen.writeStringField('payloadType', value.payload.class.name)
        gen.writeObjectField('metadata', value.metadata)
        gen.writeEndObject
    }

}

class DomainEventMessageSerializer implements Serializer<DomainEventMessage<? extends Event>> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override serialize(String topic, DomainEventMessage<? extends Event> data) {
        Json.mapper.writeValueAsBytes(data)
    }

}

class AggregateRootSerializer implements Serializer<AggregateRoot> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override serialize(String topic, AggregateRoot data) {
        val baos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(baos)
        out.writeObject(data)
        baos.toByteArray => [
            out.close
            baos.close
        ]
    }
}

class JavaSerializer<T extends Serializable> implements Serializer<T> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override serialize(String topic, T data) {
        val baos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(baos)
        out.writeObject(data)
        baos.toByteArray => [
            out.close
            baos.close
        ]
    }
}