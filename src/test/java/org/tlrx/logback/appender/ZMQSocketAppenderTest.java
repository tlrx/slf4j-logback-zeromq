package org.tlrx.logback.appender;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author tlrx
 */
public class ZMQSocketAppenderTest extends TestCase {

    private static final String CONNECT = "tcp://127.0.0.1:9797";

    ZMQ.Context context;
    ZMQ.Socket subscriber;

    @Before
    public void setUp() {
        context = ZMQ.context(1);
        subscriber = context.socket(ZMQ.SUB);
        subscriber.connect(CONNECT);
        subscriber.subscribe("ERROR".getBytes());
    }

    @After
    public void tearDown() {
        subscriber.close();
        context.term();
    }

    private void receiveAndAssert(String expected) {

        byte[] rec = subscriber.recv(ZMQ.NOBLOCK);

        if (rec != null) {
            String received = new String(rec);
            assertEquals(expected, received);
        } else if (expected != null) {
            Assert.fail("Nothing received!");
        }
    }


    @Test
    public void test() {

        // Creates a logger (and bind a PUB socket)
        Logger logger = LoggerFactory.getLogger(ZMQSocketAppenderTest.class);

        for (int count = 0; count < 10; count++) {

            if ((count % 2) == 1) {
                logger.error("Log message bar {}", count);
            } else {
                logger.info("Log message foo {}", count);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Assert.fail();
            }
        }

        receiveAndAssert("ERROR o.t.l.a.ZMQSocketAppenderTest - Log message bar 1");
        receiveAndAssert("ERROR o.t.l.a.ZMQSocketAppenderTest - Log message bar 3");
        receiveAndAssert("ERROR o.t.l.a.ZMQSocketAppenderTest - Log message bar 5");
        receiveAndAssert("ERROR o.t.l.a.ZMQSocketAppenderTest - Log message bar 7");
        receiveAndAssert("ERROR o.t.l.a.ZMQSocketAppenderTest - Log message bar 9");
    }
}
