package nl.kii.service.content.domain

import java.net.URL
import java.time.Instant
import java.util.Iterator
import java.util.UUID
import nl.kii.eventsourcing.Event
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.EqualsHashCode
import org.eclipse.xtend.lib.annotations.ToString
import nl.kii.eventsourcing.AggregateRoot

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class Article extends AggregateRoot {

    var String title
    var String description
    var Instant published
    var URL url
    var boolean enabled
    
    new(UUID id, String title, String description, Instant published, URL url) {
       apply(new ArticleAdded(id, title, description, published, url)) 
    }
    
    new(Iterator<Event> events) {
        super(events)
    }
    
    def disable() {
        if (this.enabled) 
            apply(new ArticleDisabled)
        this
    }
    
    def protected void handle(ArticleAdded event) {
        this.id = event.id
        this.title = event.title
        this.description = event.description
        this.published = event.published
        this.url = event.url
        this.enabled = true
    }
    
    def protected void handle(ArticleDisabled event) {
        this.enabled = false
    }
    
}