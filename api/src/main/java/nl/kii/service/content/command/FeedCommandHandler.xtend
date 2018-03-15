package nl.kii.service.content.command

import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.AsyncResult
import io.vertx.core.Handler

@ProxyGen
interface FeedCommandHandler {
    
    val static NAME = 'content.service/feed' // The name of the event bus service.

    val static ADDRESS = 'content.service/feed' // The address on which the service is published.
    
    def void addRssFeed(AddRssCommand cmd, Handler<AsyncResult<Void>> result)
    
    def void removeRssFeed(RemoveRssCommand cmd, Handler<AsyncResult<Void>> result)

}