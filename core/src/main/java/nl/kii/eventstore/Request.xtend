package nl.kii.eventstore

import io.vertx.core.json.JsonObject
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot
import org.eclipse.xtend.lib.annotations.Data
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension java.util.UUID.fromString

@Data
@FinalFieldsConstructor
package class Request {
    val UUID id
    val Class<? extends AggregateRoot> clazz

    new(JsonObject json) {
        this.id = json.getString('id').fromString
        try {
            this.clazz = Class.forName(json.getString(('clazz'))) as Class<? extends AggregateRoot>
        } catch (Exception e) {
            throw new IllegalStateException('Unable to convert into a value object.', e)
        }
    }

    def toJson() {
        new JsonObject => [
            put('id', id.toString)
            put('clazz', clazz.name)
        ]
    }
}
