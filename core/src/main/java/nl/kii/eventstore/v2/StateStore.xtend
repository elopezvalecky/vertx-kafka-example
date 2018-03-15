package nl.kii.eventstore.v2

import io.vertx.core.Future
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot

interface StateStore {
    
    def <AR extends AggregateRoot> Future<AR> load(UUID id, Class<AR> type)

}