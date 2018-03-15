package nl.kii.eventstore

import io.vertx.core.Future
import java.util.Map
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot

interface Repository {
        
    def <AR extends AggregateRoot> Future<Void> save(AR aggregate, Map<String,?> metadata)
    
    def <AR extends AggregateRoot> Future<AR> load(UUID id, Class<AR> aggregate)

}