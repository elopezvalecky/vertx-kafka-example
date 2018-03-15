package nl.kii.service.content

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.BodyHandler
import nl.kii.io.vertx.microservice.common.RestAPIVerticle

import static extension io.vertx.core.logging.LoggerFactory.getLogger
import static extension io.vertx.ext.web.Router.router

class ContentRestAPIVerticle extends RestAPIVerticle {
    
    val logger = class.logger
    
    val static SERVICE_NAME = 'content.service:rest-api'

    override start(Future<Void> startFuture) throws Exception {
        super.start

        extension val router = vertx.router

        route.handler(BodyHandler.create)
        
        enableCorsSupport(router)
        enableHeartbeatCheck(router, new JsonObject)
        
        exceptionHandler [logger.error('Something went wrong', it)]
        
        // get HTTP host and port from configuration, or use default value
        val host = config.getString('http.address', '0.0.0.0')
        val port = config.getInteger('http.port', 8080)

        createHttpServer(router, host, port)
            .compose[publishHttpEndpoint(SERVICE_NAME, host, port)]
            .setHandler(startFuture.completer)
    }

}