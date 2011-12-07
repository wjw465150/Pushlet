// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import nl.justobjects.pushlet.util.Log;

/**
 * Abstract Event source from which Events are pulled.
 *
 * @version $Id: EventPullSource.java,v 1.15 2007/11/23 14:33:07 justb Exp $
 * @author Just van den Broecke - Just Objects &copy;
 **/

/**
 * ABC for specifc EventPullSources.
 */
abstract public class EventPullSource implements EventSource, Runnable {
  private volatile boolean alive = false;
  private volatile boolean active = false;
  private static int threadNum = 0;
  private Thread thread;

  public EventPullSource() {
  }

  abstract protected long getSleepTime();

  abstract protected Event pullEvent();

  public void start() {
    thread = new Thread(this, "EventPullSource-" + (++threadNum));
    thread.setDaemon(true);
    thread.start();
  }

  public boolean isAlive() {
    return alive;
  }

  /**
   * Stop the event generator thread.
   */
  public void stop() {
    alive = false;

    if (thread != null) {
      thread.interrupt();
      thread = null;
    }

  }

  /**
   * Activate the event generator thread.
   */
  public void activate() {
    if (active) {
      return;
    }
    active = true;
    if (!alive) {
      start();
      return;
    }
    Log.debug(getClass().getName() + ": notifying...");
    notifyAll();
  }

  /**
   * Deactivate the event generator thread.
   */
  public void passivate() {
    if (!active) {
      return;
    }
    active = false;
  }

  /**
   * Main loop: sleep, generate event and publish.
   */
  public void run() {
    Log.debug(getClass().getName() + ": starting...");
    alive = true;
    while (alive) {
      try {
        Thread.sleep(getSleepTime());

        // Stopped during sleep: end loop.
        if (!alive) {
          break;
        }

        // If passivated wait until we get
        // get notify()-ied. If there are no subscribers
        // it wasts CPU to remain producing events...
        synchronized (this) {
          while (!active) {
            Log.debug(getClass().getName() + ": waiting...");
            wait();
          }
        }

      } catch (InterruptedException e) {
        break;
      }

      try {
        // Derived class should produce an event.
        Event event = pullEvent();
        if (null == event) {  //@wjw_add 判断当event为空时是没有合适的消息,不予处理
          continue;
        }

        // Let the publisher push it to subscribers.
        Dispatcher.getInstance().multicast(event); //TODO@ 把event广播出去
      } catch (Throwable t) {
        Log.warn("EventPullSource exception while multicasting ", t);
        t.printStackTrace();
      }
    }
    Log.debug(getClass().getName() + ": stopped");
  }
}
