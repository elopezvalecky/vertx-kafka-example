package nl.kii.eventsourcing

import java.time.Instant
import java.util.Map
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Data

@Data
class GenericEventMessage<T> extends GenericMessage<T> implements EventMessage<T> {
    
    val Instant timestamp
    
    @SuppressWarnings('unchecked')
    def static <T> EventMessage<T> asEventMessage(Object event) {
        if (EventMessage.isInstance(event)) {
            return event as EventMessage<T>
        } else if (event instanceof Message<?>) {
            val message = event as Message<?>
            return new GenericEventMessage<T>(message.payload as T, message.metadata)
        }
        return new GenericEventMessage<T>(event as T);
    }    

    new(UUID id, Instant timestamp, T payload, Map<String, ?> metadata) {
        super(id, payload, metadata)
        this.timestamp = timestamp
    }
    
    new(GenericEventMessage<T> original, Map<String, ?> metadata) {
        this(original.id, original.timestamp, original.payload, metadata)
    }

    new(T payload, Map<String, ?> metadata) {
        this(UUID.randomUUID, Instant.now, payload, metadata)
    }

    new(T payload) {
        this(payload, emptyMap)
    }
    
    override withMetaData(Map<String,?> metadata) {
        if (this.metadata == metadata) return this
        new GenericEventMessage<T>(this, metadata)
    }
    
    override andMetaData(Map<String,?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericEventMessage<T>(this, tmp)
    }

    override toString() '''«class.simpleName»[«payload.toString»]'''

}