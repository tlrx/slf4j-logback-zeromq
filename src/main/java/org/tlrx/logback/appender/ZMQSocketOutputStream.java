package org.tlrx.logback.appender;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author tlrx
 * @author prixeus
 */
public class ZMQSocketOutputStream extends OutputStream {

    private final ZMQ.Socket socket;
    private final boolean sendMultipartEnabled;
    private String[] prefixes = null;

    public ZMQSocketOutputStream(ZMQ.Socket socket) {
        this.socket = socket;
        this.sendMultipartEnabled = false;
    }

    public ZMQSocketOutputStream(ZMQ.Socket socket, String[] prefixes) {
        this.socket = socket;
        this.sendMultipartEnabled = true;
        this.prefixes = prefixes;
    }

    private void sendMultipart(byte[] bytes) {
        ZMsg msg = new ZMsg();

        for (String prefix: prefixes) {
            msg.addLast(prefix);
        }
        msg.addLast(bytes);

        msg.send(this.socket);
    }

    @Override
    public void write(int i) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(i);

        if (sendMultipartEnabled) {
            sendMultipart(b.array());
        } else {
            socket.send(b.array(), 0);
        }
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        if (sendMultipartEnabled) {
            sendMultipart(bytes);
        } else {
            socket.send(bytes, 0);
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
