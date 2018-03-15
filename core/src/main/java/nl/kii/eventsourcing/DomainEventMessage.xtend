package nl.kii.eventsourcing

import java.util.UUID

/**
 * Represents a Message that wraps a DomainEvent and an Event representing an important change in the Domain. In
 * contrast to a regular EventMessage, a DomainEventMessages contains the identifier of the Aggregate that reported it.
 * The DomainEventMessage's sequence number allows Messages to be placed in their order of generation.
 *
 * @param <T> The type of payload contained in this Message
 * @since 1.0
 */
interface DomainEventMessage<T> extends EventMessage<T> {
    
    /**
     * Returns the identifier of the Aggregate that generated this DomainEvent. Note that the value returned does not
     * necessarily have to be the same instance that was provided at creation time.
     *
     * @return the identifier of the Aggregate that generated this DomainEvent
     */
    def UUID getAggregateId()

    def String getAggregateType()

}