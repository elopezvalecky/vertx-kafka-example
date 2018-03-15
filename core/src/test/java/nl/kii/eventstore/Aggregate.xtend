package nl.kii.eventstore

import java.util.Iterator
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot
import nl.kii.eventsourcing.Event
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.EqualsHashCode
import org.eclipse.xtend.lib.annotations.ToString

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class Demo extends AggregateRoot {

    var String text
    var int number
    var boolean flag

    new(UUID id, String text) {
        apply(new DemoAdded(id, text))
    }

    new(Iterator<Event> events) {
        super(events)
    }

    def text(String text) {
        apply(new DemoTextUpdated(text))
        this
    }

    def increase() {
        apply(new DemoNumberIncreased)
        this
    }

    def decrease() {
        apply(new DemoNumberDecreased)
        this
    }

    def toggle() {
        apply(new DemoFlagToggled)
        this
    }

    def protected void handle(DemoAdded event) {
        this.id = event.id
        this.text = event.text
        this.number = 0
        this.flag = true
    }

    def protected void handle(DemoTextUpdated event) {
        this.text = event.text
    }

    def protected void handle(DemoNumberIncreased event) {
        this.number = this.number + 1
    }

    def protected void handle(DemoNumberDecreased event) {
        this.number = this.number - 1
    }

    def protected void handle(DemoFlagToggled event) {
        this.flag = !this.flag
    }

}

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class DemoAdded extends Event {
    var UUID id
    var String text

    private new() {
    }

    new(UUID id, String text) {
        this.id = id
        this.text = text
    }
}

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class DemoFlagToggled extends Event {
}

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class DemoNumberIncreased extends Event {
}

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class DemoNumberDecreased extends Event {
}

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class DemoTextUpdated extends Event {
    var String text

    private new() {
    }

    new(String text) {
        this.text = text
    }
}
