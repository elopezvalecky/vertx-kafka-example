package nl.kii.eventstore

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.serviceproxy.ServiceException
import io.vertx.serviceproxy.ServiceExceptionMessageCodec

class StateStoreVertxEBProxy implements StateStore {
    
    val Vertx _vertx
    val String _address
    val DeliveryOptions _options
    var boolean closed

    new(Vertx vertx, String address) {
        this(vertx, address, null)
    }

    new(Vertx vertx, String address, DeliveryOptions options) {
        this._vertx = vertx
        this._address = address
        this._options = options
        try {
            this._vertx.eventBus.registerDefaultCodec(ServiceException, new ServiceExceptionMessageCodec)
        } catch (IllegalStateException ex) {
        }
    }

    override void load(Request request, Handler<AsyncResult<Buffer>> result) {
        if (closed) {
            result.handle(Future.failedFuture(new IllegalStateException('Proxy is closed')))
            return
        }
        var JsonObject _json = new JsonObject => [
            put('request', request?.toJson)
        ]
        var _deliveryOptions = if(_options !== null) new DeliveryOptions(_options) else new DeliveryOptions
        _deliveryOptions.addHeader('action', 'load')
        _vertx.eventBus.<Buffer>send(_address, _json, _deliveryOptions) [
            if (failed)
                result.handle(Future.failedFuture(cause))
            else
                result.handle(Future.succeededFuture(it.result.body))
        ]
    }

}
