// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import java.io.UnsupportedEncodingException;

import nl.justobjects.pushlet.redis.RedisManager;

/**
 * FIFO queue with guarded suspension. <b>Purpose</b><br>
 * <p/>
 * <b>Implementation</b><br>
 * FIFO queue class implemented with circular array. The enQueue() and deQueue()
 * methods use guarded suspension according to a readers/writers pattern,
 * implemented with java.lang.Object.wait()/notify().
 * <p/>
 * <b>Examples</b><br>
 * <p/>
 * <br>
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: EventQueue.java,v 1.3 2007/11/23 14:33:07 justb Exp $
 */
public class EventQueue { //@wjw_node 属于 Subscriber 的事件队列
  static RedisManager redis;
  static {
    redis = RedisManager.getInstance();
  }
  private static final String REDIS_EVENTQUEUE_PREFIX = "pushlet:eventqueue:";
  private static final int SLEEP_TIME = 200;

  /**
   * Defines maximum queue size
   */
  private int capacity = 256;

  private String redis_eventqueue_key;

  /**
   * Construct queue with default (8) capacity.
   */
  public EventQueue(String aSessionId) {
    this(aSessionId, 256);
  }

  /**
   * Construct queue with specified capacity.
   */
  public EventQueue(String aSessionId, int capacity) {
    this.capacity = capacity;

    redis_eventqueue_key = REDIS_EVENTQUEUE_PREFIX + aSessionId;
  }

  /**
   * Put item in queue; waits() indefinitely if queue is full.
   */
  public boolean enQueue(Event item) throws InterruptedException {
    return enQueue(item, -1);
  }

  /**
   * Put item in queue; if full wait maxtime.
   */
  public boolean enQueue(Event item, long maxWaitTime) throws InterruptedException {
    if (maxWaitTime < 0) {
      while (isFull()) {
        Thread.sleep(SLEEP_TIME);
      }
    } else {
      long remainTime = maxWaitTime;
      while (isFull() && remainTime >= 0) {
        Thread.sleep(SLEEP_TIME);
        remainTime = remainTime - SLEEP_TIME;
      }

      if (isFull()) {
        return false;
      }
    }

    // Put item in queue
    redis.lpush(redis_eventqueue_key, redis.toXML(item));

    return true;
  }

  /**
   * Get head; if empty wait until something in queue.
   */
  public Event deQueue() throws InterruptedException {
    return deQueue(-1);
  }

  /**
   * Get head; if empty wait for specified time at max.
   */
  public Event deQueue(long maxWaitTime) throws InterruptedException {
    if (maxWaitTime < 0) {
      while (isEmpty()) {
        Thread.sleep(SLEEP_TIME);
      }
    } else {
      long remainTime = maxWaitTime;
      while (isEmpty() && remainTime >= 0) {
        Thread.sleep(SLEEP_TIME);
        remainTime = remainTime - SLEEP_TIME;
      }

      if (isEmpty()) {
        return null;
      }
    }

    // Dequeue item
    Event result = fetchNext();

    // Return dequeued item
    return result;
  }

  /**
   * Get all queued Events.
   */
  public Event[] deQueueAll(long maxWaitTime) throws InterruptedException {
    if (maxWaitTime < 0) {
      while (isEmpty()) {
        Thread.sleep(SLEEP_TIME);
      }
    } else {
      long remainTime = maxWaitTime;
      while (isEmpty() && remainTime >= 0) {
        Thread.sleep(SLEEP_TIME);
        remainTime = remainTime - SLEEP_TIME;
      }

      if (isEmpty()) {
        return null;
      }
    }

    // Dequeue all items item
    String strEvent = null;
    java.util.List<Event> listEvent = new java.util.ArrayList<Event>(this.getSize());
    while ((strEvent = redis.lpop(redis_eventqueue_key)) != null) {
      listEvent.add((Event) redis.fromXML(strEvent));
    }
    Event[] events = listEvent.toArray(new Event[0]);

    // Return dequeued item
    return events;
  }

  public int getSize() {
    return redis.llen(redis_eventqueue_key).intValue();
  }

  /**
   * Is the queue empty ?
   */
  public boolean isEmpty() {
    return redis.llen(redis_eventqueue_key).intValue() == 0;
  }

  /**
   * Is the queue full ?
   */
  public boolean isFull() {
    return redis.llen(redis_eventqueue_key).intValue() == capacity;
  }

  /**
   * Circular counter.
   */
  private Event fetchNext() {
    return (Event) redis.fromXML(redis.lpop(redis_eventqueue_key));
  }

}
