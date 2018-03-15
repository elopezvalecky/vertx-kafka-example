package nl.kii.eventsourcing

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import java.time.Instant
import java.util.Map
import java.util.UUID
import nl.kii.eventstore.DomainEventMessageJsonDeserializer
import nl.kii.eventstore.DomainEventMessageJsonSerializer
import org.eclipse.xtend.lib.annotations.Data

@Data
@JsonSerialize(using=DomainEventMessageJsonSerializer)
@JsonDeserialize(using=DomainEventMessageJsonDeserializer)
class GenericDomainEventMessage<T> extends GenericEventMessage<T> implements DomainEventMessage<T> {

    val UUID aggregateId
    val String aggregateType
    
    new(UUID id, Instant timestamp, UUID aggregateId, String aggregateType, T payload, Map<String, ?> metadata) {
        super(id, timestamp, payload, metadata)
        this.aggregateId = aggregateId
        this.aggregateType = aggregateType
    }
    
    new(GenericDomainEventMessage<T> original, Map<String, ?> metadata) {
        this(original.id, original.timestamp, original.aggregateId, original.aggregateType, original.payload, metadata)
    }

    new(UUID aggregateId, String aggregateType, T payload, Map<String, ?> metadata) {
        this(UUID.randomUUID, Instant.now, aggregateId, aggregateType, payload, metadata)
    }

    new(UUID aggregateId, String aggregateType, T payload) {
        this(aggregateId, aggregateType, payload, emptyMap)
    }

    override withMetaData(Map<String,?> metadata) {
        if (this.metadata == metadata) return this
        new GenericDomainEventMessage<T>(this, metadata)
    }
    
    override andMetaData(Map<String,?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericDomainEventMessage<T>(this, tmp)
    }

    override toString() '''«class.simpleName»[«payload.toString»]'''

}