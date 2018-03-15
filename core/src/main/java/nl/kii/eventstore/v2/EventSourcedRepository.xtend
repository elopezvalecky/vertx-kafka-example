package nl.kii.eventstore.v2

import nl.kii.eventsourcing.AggregateRoot
import java.util.Map
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Accessors
import javax.annotation.Resource
import nl.kii.eventsourcing.GenericDomainEventMessage
import io.vertx.core.Future

class EventSourcedRepository implements Repository {
    
    @Resource
    @Accessors(PUBLIC_SETTER)
    var StateStore stateStore
    
    @Resource
    @Accessors(PUBLIC_SETTER)
    var EventStore eventStore
    
    override <AR extends AggregateRoot> save(AR aggregate, Map<String, ?> metadata) {
        val future = Future.future
        val events = aggregate
                        .unsaved
                        .iterator
                        .map [new GenericDomainEventMessage(aggregate.id, aggregate.class.name, it, metadata?:emptyMap)]
        eventStore
            .save(events)
            .setHandler(future.completer)
        future
    }
    
    override <AR extends AggregateRoot> load(UUID id, Class<AR> type) {
        stateStore.<AR>load(id, type)
    }
    
}