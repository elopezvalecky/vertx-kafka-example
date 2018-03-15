package nl.kii.service.content.domain

import java.net.URL
import java.util.UUID
import nl.kii.eventsourcing.Event
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.EqualsHashCode
import org.eclipse.xtend.lib.annotations.ToString

@ToString
@EqualsHashCode
@Accessors(PUBLIC_GETTER)
class RssAdded extends Event {
    
    var UUID id
    var String title
    var URL url

    private new() {}
    
    new(UUID id, String title, URL url) {
        this.id = id
        this.title = title
        this.url = url
    }
    
}