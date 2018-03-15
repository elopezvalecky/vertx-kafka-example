package nl.kii.service.content.command

import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import nl.kii.xtend.cqrses.CommandHandler

@ProxyGen
interface ItemCommandHandler extends CommandHandler {
    
    val static NAME = 'content.service/item' // The name of the event bus service.

    val static ADDRESS = 'content.service/item' // The address on which the service is published.
    
    def void addArticle(AddArticle cmd, Handler<AsyncResult<Void>> result)
    
}