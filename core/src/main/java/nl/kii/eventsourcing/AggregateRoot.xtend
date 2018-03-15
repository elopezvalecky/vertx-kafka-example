package nl.kii.eventsourcing

import java.io.IOException
import java.io.ObjectInputStream
import java.util.Iterator
import java.util.List

abstract class AggregateRoot extends Entity {

    var transient List<Event> unsaved = newArrayList

    protected new() {}

    new(Iterator<Event> events) {
        events.forEach[on]
    }

    def private void on(Event event) {
        val method = try {
            class.getDeclaredMethod('handle', event.class)
        } catch(NoSuchMethodException e ) {
            null
        }
        if (method !== null) {
            method.accessible = true
            try {
                method.invoke(event)
            } catch (Exception e) {
                throw new RuntimeException('''Unable to call event handler method for «event.class.name»''', e)
            }
        }
    }

    def protected void apply(Event event) {
        on(event)
        unsaved.add(event)
    }

    def getUnsaved() {
        this.unsaved.unmodifiableView
    }

    def private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject
        unsaved = newArrayList
    }

}
