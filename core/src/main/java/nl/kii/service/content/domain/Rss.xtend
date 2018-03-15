package nl.kii.service.content.domain

import java.net.URL
import java.util.Iterator
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot
import nl.kii.eventsourcing.Event
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.ToString
import org.eclipse.xtend.lib.annotations.EqualsHashCode

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class Rss extends AggregateRoot {
    
    var String title
    var URL url
    var boolean enabled
    
    new(UUID id, String title, URL url) {
        apply(new RssAdded(id, title, url))
    }
    
    new(Iterator<Event> events) {
        super(events)
    }
    
    def disable() {
        if (this.enabled) 
            apply(new RssDisabled)
        this
    }
    
    def protected handle(RssAdded event) {
        this.id = event.id
        this.title = event.title
        this.url = event.url
        this.enabled = true
    }

    def protected handle(RssDisabled event) {
        this.enabled = false
    }

}