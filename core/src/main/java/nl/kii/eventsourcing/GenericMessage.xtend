package nl.kii.eventsourcing

import java.util.HashMap
import java.util.Map
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Data

@Data
class GenericMessage<T> implements Message<T> {
    
    val UUID id
    val Map<String,?> metadata
    val T payload
    
    new(UUID id, T payload, Map<String,?> metadata) {
        this.id = id
        this.payload = payload
        this.metadata = metadata.unmodifiableView
    }
    
    new(Message<T> original, Map<String,?> metadata) {
        this(original.id, original.payload, new HashMap<String, Object>(metadata))
    }
    
    new(T payload, Map<String,?> metadata) {
        this(UUID.randomUUID, payload, metadata)
    }
    
    new(T payload) {
        this(payload, emptyMap)
    }
    
    override withMetaData(Map<String,?> metadata) {
        if (this.metadata == metadata) return this
        new GenericMessage<T>(this, metadata)
    }
    
    override andMetaData(Map<String,?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericMessage<T>(this, tmp)
    }
    
    override toString() '''«class.simpleName»[«payload.toString»]'''
    
    override getPayloadType() {
        this.payload.class as Class<T>
    }

}