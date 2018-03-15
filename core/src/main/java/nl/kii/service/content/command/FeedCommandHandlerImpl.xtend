package nl.kii.service.content.command

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import nl.kii.eventstore.Repository
import nl.kii.service.content.domain.Rss
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class FeedCommandHandlerImpl implements FeedCommandHandler {
    
    val Repository repository
    
    override addRssFeed(extension AddRssCommand cmd, Handler<AsyncResult<Void>> result) {
        val future = Future.succeededFuture
        future
            .map[new Rss(id, title, url)]
            .compose[feed|repository.save(feed, null)]
        result.handle(future)
    }
    
    override removeRssFeed(extension RemoveRssCommand cmd, Handler<AsyncResult<Void>> result) {
        val future = Future.succeededFuture
        future
            .compose[repository.load(id, Rss)]
            .map[disable]
            .compose[feed|repository.save(feed, null)]
        result.handle(future)
    }
    
}
