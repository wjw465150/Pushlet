package nl.justobjects.pushlet.test;

import nl.justobjects.pushlet.client.PushletClient;
import nl.justobjects.pushlet.client.PushletClientListener;
import nl.justobjects.pushlet.core.Event;
import nl.justobjects.pushlet.core.Protocol;
import nl.justobjects.pushlet.util.PushletException;

public class SimpleTest implements PushletClientListener, Protocol {
  private static String SUBJECT = "/pushlet/ping,/user/login,/user/chat";
  private static final String MODE = MODE_STREAM;
  private static final String myId = "admin";

  /** Error occurred. */
  public void onError(String message) {
    p(message);
  }

  /** Abort event from server. */
  public void onAbort(Event theEvent) {
    p("onAbort received: " + theEvent);
  }

  /** Data event from server. */
  public void onData(Event theEvent) {
    p("onData received: " + theEvent.toXML());
  }

  /** Heartbeat event from server. */
  public void onHeartbeat(Event theEvent) {
    p("onHeartbeat received: " + theEvent);
  }

  /** Generic print. */
  public void p(String s) {
    System.out.println("[SimpleTest] " + s);
  }

  public SimpleTest(String aHost, int aPort) {
    // Create and start a Pushlet client; we receive callbacks
    // through onHeartbeat() and onData().
    try {
      PushletClient pushletClient = new PushletClient(aHost, aPort);
      pushletClient.setDebug(false);
      pushletClient.joinListen(myId, false, this, MODE, SUBJECT);
      p("pushletClient started");
    } catch (PushletException pe) {
      p("Error in setting up pushlet session pe=" + pe);
    }
  }

  public static void main(String args[]) {
    SimpleTest test = new SimpleTest("localhost", 80);
  }

}
