package nl.kii.service.content

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.Future
import io.vertx.core.json.Json
import io.vertx.serviceproxy.ServiceBinder
import nl.kii.eventstore.EventSourcedRepository
import nl.kii.io.vertx.microservice.common.BaseMicroserviceVerticle
import nl.kii.service.content.command.FeedCommandHandler
import nl.kii.service.content.command.FeedCommandHandlerImpl

import static extension io.vertx.core.logging.LoggerFactory.getLogger

class ContentVerticle extends BaseMicroserviceVerticle {

    val static TOPIC = 'content-service-events'
    val static STORE = 'content-service-aggregates'
    
    val logger = class.logger

    var EventSourcedRepository repository 

    override start(Future<Void> startFuture) throws Exception {
        super.start
        logger.info('Starting service')
        
        // Setting up jackson 
        Json.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        Json.prettyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        
        val topic = config.getString('service.topic', TOPIC)
        val store = config.getString('service.store', STORE)
        val kafkaProperties = config.getJsonObject('kafka')

        this.repository = new EventSourcedRepository(vertx, topic, store, kafkaProperties.map)
        
        // Initializing command handlers
        val feedCommandHandler = new FeedCommandHandlerImpl(this.repository)
        
        new ServiceBinder(vertx).setAddress(FeedCommandHandler.ADDRESS).register(FeedCommandHandler, feedCommandHandler)
        Future.succeededFuture
            .compose [ publishEventBusService(FeedCommandHandler.NAME, FeedCommandHandler.ADDRESS, FeedCommandHandler) ]        
            .setHandler(startFuture.completer)
    }

    override stop() throws Exception {
        this.repository.close
        super.stop
    }
    
}