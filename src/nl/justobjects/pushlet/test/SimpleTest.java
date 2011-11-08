package nl.justobjects.pushlet.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import nl.justobjects.pushlet.client.PushletClient;
import nl.justobjects.pushlet.client.PushletClientListener;
import nl.justobjects.pushlet.core.Event;
import nl.justobjects.pushlet.core.Protocol;
import nl.justobjects.pushlet.util.PushletException;

public class SimpleTest extends TimerTask implements PushletClientListener, Protocol {
  private static String SUBJECT = "/pushlet/ping,/user/login,/user/chat";
  private static final String MODE = MODE_STREAM;
  private static final String myId = null;  //"admin";

  private PushletClient pushletClient;
  private Timer timer;
  private final long TIMER_INTERVAL_MILLIS = 5 * 1000;

  /** Error occurred. */
  public void onError(String message) {
    p(message);
  }

  /** Abort event from server. */
  public void onAbort(Event theEvent) {
    p("onAbort received: " + theEvent);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  /** Data event from server. */
  public void onData(Event theEvent) {
    p("onData received: " + theEvent.toXML());
    if (theEvent.getSubject().equals("/user/chat")) {
      Map theAttributes = new HashMap(3);
      theAttributes.put("message", "echo:" + theEvent.getField("message"));
      theAttributes.put(P_FROM, pushletClient.getId());
      theAttributes.put(P_TO, theEvent.getField(P_FROM));
      try {
        pushletClient.publish(theEvent.getSubject(), theAttributes);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  /** Heartbeat event from server. */
  public void onHeartbeat(Event theEvent) {
    p("onHeartbeat received: " + theEvent);
  }

  /** Generic print. */
  public void p(String s) {
    System.out.println("[SimpleTest] " + s);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void run() {
    if (pushletClient != null) {
      Map theAttributes = new HashMap(1);
      theAttributes.put("name", pushletClient.getId());
      try {
        pushletClient.publish_to_online("/user/login", theAttributes);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  public SimpleTest(String aHost, int aPort) {
    // Create and start a Pushlet client; we receive callbacks
    // through onHeartbeat() and onData().
    try {
      pushletClient = new PushletClient(aHost, aPort);
      pushletClient.setDebug(false);
      pushletClient.joinListen(myId, false, this, MODE, SUBJECT);
      p("pushletClient started");

      if (timer != null) {
        timer.cancel();
        timer = null;
      }
      timer = new Timer(false);
      timer.schedule(this, TIMER_INTERVAL_MILLIS, TIMER_INTERVAL_MILLIS);
    } catch (PushletException pe) {
      pushletClient = null;
      p("Error in setting up pushlet session pe=" + pe);
    }
  }

  public static void main(String args[]) {
    SimpleTest test;
    if (args.length == 0) {
      test = new SimpleTest("localhost", 80);
    } else if (args.length == 1) {
      test = new SimpleTest("localhost", Integer.parseInt(args[0]));
    } else {
      test = new SimpleTest(args[0], Integer.parseInt(args[1]));
    }
  }

}
