// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import nl.justobjects.pushlet.redis.RedisManager;
import nl.justobjects.pushlet.util.PushletException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Represents single subject subscription
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: Subscription.java,v 1.5 2007/11/23 14:33:07 justb Exp $
 */
public class Subscription implements ConfigDefs {
  static RedisManager redis = RedisManager.getInstance();

  public static final int ID_SIZE = 8;
  public static final String SUBJECT_SEPARATOR = ",";

  private String subject; //@wjw_node 此字段不使用,而是使用subjects字段
  private String[] subjects; // We may subscribe to multiple subjects by separating
                             // them with SUBJECT_SEPARATOR, e.g. "/stocks/aex,/system/memory,..").

  /**
   * Optional label, a user supplied token.
   */
  private String label;

  /**
   * Protected constructor as we create through factory method.
   */
  protected Subscription() {
  }

  /**
   * Create instance through factory method.
   * 
   * @param aSubject
   *          the subject (topic).
   * @return a Subscription object (or derived)
   * @throws nl.justobjects.pushlet.util.PushletException
   *           exception, usually misconfiguration
   */
  public static Subscription create(String aSubject) throws PushletException {
    return create(aSubject, null);
  }

  /**
   * Create instance through factory method.
   * 
   * @param aSubject
   *          the subject (topic).
   * @param aLabel
   *          the subject label (optional).
   * @return a Subscription object (or derived)
   * @throws nl.justobjects.pushlet.util.PushletException
   *           exception, usually misconfiguration
   */
  public static Subscription create(String aSubject, String aLabel) throws PushletException {
    if (aSubject == null || aSubject.length() == 0) {
      throw new IllegalArgumentException("Null or emtpy subject");
    }

    Subscription subscription;
    try {
      subscription = (Subscription) Config.getClass(SUBSCRIPTION_CLASS, "nl.justobjects.pushlet.core.Subscription").newInstance();
    } catch (Throwable t) {
      throw new PushletException("Cannot instantiate Subscriber from config", t);
    }

    // Init
    subscription.subject = aSubject;

    // We may subscribe to multiple subjects by separating
    // them with SUBJECT_SEPARATOR, e.g. "/stocks/aex,/system/memory,..").
    subscription.subjects = aSubject.split(SUBJECT_SEPARATOR);

    subscription.label = aLabel;
    return subscription;
  }

  public String getLabel() {
    return label;
  }

  public String getSubject() {
    return subject;
  }

  public String[] getSubjects() {
    return subjects;
  }

  public String toJsonString() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("subject", subject);
    node.put("label", label);

    return node.toString();
  }

  public static Subscription fromJsonString(String content) throws PushletException {
    JsonNode node = redis.readTree(content);
    return create(node.get("subject").getTextValue(), node.get("label").getTextValue());
  }
}
