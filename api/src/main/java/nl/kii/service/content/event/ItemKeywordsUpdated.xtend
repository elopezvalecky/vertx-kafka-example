package nl.kii.service.content.event

import java.util.List
import java.util.UUID
import nl.kii.service.content.domain.TextWeight
import nl.kii.xtend.cqrses.Event
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors(PUBLIC_GETTER)
class ItemKeywordsUpdated implements Event {
    
    var UUID id
    var List<TextWeight> kewords

    private new() {}
    new(UUID id, List<TextWeight> kewords) {
        this.kewords = kewords.unmodifiableView
    }

}