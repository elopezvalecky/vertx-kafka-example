package nl.kii.eventstore.v2

import io.vertx.core.Future
import nl.kii.eventsourcing.EventMessage
import java.util.Iterator
import nl.kii.eventsourcing.Event

interface EventStore {
    
    def <M extends EventMessage<? extends Event>> Future<Void> save(Iterator<M> events)
    
}