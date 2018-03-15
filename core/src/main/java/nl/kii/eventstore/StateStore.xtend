package nl.kii.eventstore

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer

package interface StateStore {
    def void load(Request request, Handler<AsyncResult<Buffer>> result)
}
