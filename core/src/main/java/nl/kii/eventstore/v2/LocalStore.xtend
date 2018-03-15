package nl.kii.eventstore.v2

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot
import io.vertx.core.buffer.Buffer

package interface LocalStore {
    
    def <AR extends AggregateRoot> void load(UUID id, Class<AR> type, Handler<AsyncResult<Buffer>> result)
    
}