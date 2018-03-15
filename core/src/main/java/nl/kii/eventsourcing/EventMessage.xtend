package nl.kii.eventsourcing

import java.time.Instant

/**
 * Represents a Message wrapping an Event, which is represented by its payload. An Event is a representation of an
 * occurrence of an event (i.e. anything that happened any might be of importance to any other component) in the
 * application. It contains the data relevant for components that need to act based on that event.
 *
 * @param <T> The type of payload contained in this Message
 * @see DomainEventMessage
 * @since 1.0
 */
interface EventMessage<T> extends Message<T>{
    
    /**
     * Returns the timestamp of this event. The timestamp is set to the date and time the event was reported.
     *
     * @return the timestamp of this event.
     */
    def Instant getTimestamp()

}