package nl.kii.service.content.domain

import java.time.Instant
import java.util.List
import nl.kii.service.content.event.ItemKeywordsUpdated
import nl.kii.xtend.cqrses.AggregateRoot
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors(PUBLIC_GETTER)
abstract class Item extends AggregateRoot {
    
    var protected Instant published
    var protected List<TextWeight> keywords
    var protected List<TextWeight> entities = newArrayList
    var protected List<Object> classification = newArrayList

    def addKewords(List<TextWeight> keywords) {
        apply(new ItemKeywordsUpdated(id, keywords))
        this
    }

    def private void handle(ItemKeywordsUpdated event) {
        keywords.addAll(keywords)
    }

}