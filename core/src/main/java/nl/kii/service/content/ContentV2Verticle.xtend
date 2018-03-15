package nl.kii.service.content

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.Future
import io.vertx.core.json.Json
import nl.kii.io.vertx.microservice.common.BaseMicroserviceVerticle

import static extension io.vertx.core.logging.LoggerFactory.getLogger

class ContentV2Verticle extends BaseMicroserviceVerticle {
    
    val static TOPIC = 'content-service-events'
    val static STORE = 'content-service-aggregates'
    
    val logger = class.logger


    override start(Future<Void> startFuture) throws Exception {
        super.start
        logger.info('Starting service')
        
        // Setting up jackson 
        Json.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        Json.prettyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        
        val topic = config.getString('service.topic', TOPIC)
        val store = config.getString('service.store', STORE)
        val kafkaProperties = config.getJsonObject('kafka')
        
        
        
    }

}