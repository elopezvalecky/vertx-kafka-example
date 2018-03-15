package nl.kii.eventstore.v2

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Verticle
import io.vertx.core.impl.CompositeFutureImpl
import java.beans.Statement
import java.util.ArrayList
import java.util.List
import java.util.Properties
import java.util.stream.Collectors
import nl.kii.eventsourcing.DomainEventMessage
import nl.kii.eventsourcing.Event
import nl.kii.eventstore.DomainEventMessageSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension io.vertx.core.logging.LoggerFactory.getLogger

@FinalFieldsConstructor
class KafkaProducerRunner extends AbstractVerticle {
    
    val logger = class.logger
    
    val Properties config
    val List<Verticle> verticles
    
    var KafkaProducer<String, DomainEventMessage<? extends Event>> producer
    
    override start(Future<Void> startFuture) throws Exception {
        Future.succeededFuture
            .compose[
                <Void>execute [
                    logger.info('Starting KafkaProducer')
                    initializeKafkaProducer
                    complete()
                ]
            ]
            .compose[CompositeFutureImpl.all(deploy(new ArrayList(this.verticles))).<Void>map[null]]
            .setHandler(startFuture.completer)
    }

    override stop(Future<Void> stopFuture) throws Exception {
        vertx.executeBlocking([
            logger.info('Shutting down KafkaProducer')
            producer.close
            complete()
        ], stopFuture.completer)
    }

    def private initializeKafkaProducer() {
        producer = new KafkaProducer(this.config, new StringSerializer, new DomainEventMessageSerializer)
    }

    def private <V extends Verticle> List<Future<String>> deploy(List<V> verticles) {
        verticles
            .stream
            .map [
                val future = Future.future

                new Statement(it, 'setProducer', #[this.producer]).execute

                vertx.deployVerticle(it, future.completer)
                future
            ]
            .collect(Collectors.toList)
    }
        
    def private <T> execute(Handler<Future<T>> fn) {
        val future = Future.<T>future
        vertx.executeBlocking(fn, future.completer)
        future
    }

}