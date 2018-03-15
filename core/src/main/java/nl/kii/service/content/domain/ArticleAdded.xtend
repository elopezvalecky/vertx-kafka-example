package nl.kii.service.content.domain

import java.net.URL
import java.time.Instant
import java.util.UUID
import nl.kii.eventsourcing.Event
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.EqualsHashCode
import org.eclipse.xtend.lib.annotations.ToString

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class ArticleAdded extends Event {
    
    var UUID id
    var String title
    var String description
    var Instant published
    var URL url

    private new() {}
    
    new(UUID id, String title, String description, Instant published, URL url) {
        this.id = id
        this.title = title
        this.description = description
        this.published = published
        this.url = url
    }

}