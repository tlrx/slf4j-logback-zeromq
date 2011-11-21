package org.tlrx.logback.appender;

import ch.qos.logback.core.OutputStreamAppender;
import org.zeromq.ZMQ;

/**
 * A ØMQ socket appender for Logback
 *
 * @author tlrx
 */
public class ZMQSocketAppender<E> extends OutputStreamAppender<E> {

    private static final ZMQ.Context context = ZMQ.context(1);

    private String type;
    private String bind;
    private String connect;

    private enum METHOD {
        BIND,
        CONNECT
    }

    private ZMQSocketOutputStream outputStream;

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public String getConnect() {
        return connect;
    }

    public void setConnect(String connect) {
        this.connect = connect;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public void start() {

        boolean error = false;

        // Determines the socket type
        int socketType;

        if ("sub".equalsIgnoreCase(type)) {
            socketType = ZMQ.SUB;
        } else if ("req".equalsIgnoreCase(type)) {
            socketType = ZMQ.REQ;
        } else if ("xreq".equalsIgnoreCase(type)) {
            socketType = ZMQ.XREQ;
        } else if ("pub".equalsIgnoreCase(type)) {
            socketType = ZMQ.PUB;
        } else {
            addWarn("[" + type + "] should be one of [REQ, XREQ, SUB]" + ", using default ØMQ socket type, PUB by default.");
            socketType = ZMQ.PUB;
        }

        // Determines the socket connection method (bind or connect)
        METHOD method = METHOD.BIND;
        String address = null;

        switch (socketType) {
            case ZMQ.SUB:
                // Sub sockets can connect or bind
                if (connect != null) {
                    method = METHOD.CONNECT;
                    address = connect;
                } else if (bind != null) {
                    method = METHOD.BIND;
                    address = bind;
                } else {
                    addError("Either <connect> or <bind> must not be null for SUB sockets.");
                    error = true;
                }
                break;

            case ZMQ.PUB:
                // Pub sockets can connect or bind
                if (bind != null) {
                    method = METHOD.BIND;
                    address = bind;
                } else if (connect != null) {
                    method = METHOD.CONNECT;
                    address = connect;
                } else {
                    addError("Either <connect> or <bind> must not be null for PUB sockets.");
                    error = true;
                }
                break;

            case ZMQ.REQ:
                // Req sockets can  only connect
                if (connect != null) {
                    method = METHOD.CONNECT;
                    address = connect;
                } else {
                    addError("<connect> must not be null for REQ sockets.");
                    error = true;
                }
                break;

            case ZMQ.XREQ:
                // Req sockets can  only connect
                if (connect != null) {
                    method = METHOD.CONNECT;
                    address = connect;
                } else {
                    addError("<connect> must not be null for XREQ sockets.");
                    error = true;
                }
                break;

            default:
                // Should not happen
                addError("Unexpected error, please check your configuration");
                error = true;
                break;
        }

        if (!error) {
            // Creates the zmq socket
            ZMQ.Socket socket = context.socket(socketType);

            if (method == METHOD.CONNECT) {
                socket.connect(address);
            } else {
                socket.bind(address);
            }

            // Set the socket as an OutputStream
            outputStream = new ZMQSocketOutputStream(socket);
            setOutputStream(outputStream);

            super.start();
        } else {
            addError("Socket " + getName() + " has configuration errors and is not started!");
        }
    }

    @Override
    public void stop() {
        super.stop();
    }
}
