// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Properties;

/**
 * Utilities that interact with the underlying OS/JVM.
 * 
 * @author Just van den Broecke
 * @version $Id: Sys.java,v 1.4 2007/11/10 14:17:18 justb Exp $
 */
public class Sys {

  /**
   * Replace characters having special meaning <em>inside</em> HTML tags with
   * their escaped equivalents, using character entities such as
   * <tt>'&amp;'</tt>.
   * <p/>
   * <P>
   * The escaped characters are :
   * <ul>
   * <li><
   * <li>>
   * <li>"
   * <li>'
   * <li>\
   * <li>&
   * </ul>
   * <p/>
   * <P>
   * This method ensures that arbitrary text appearing inside a tag does not
   * "confuse" the tag. For example, <tt>HREF='Blah.do?Page=1&Sort=ASC'</tt>
   * does not comply with strict HTML because of the ampersand, and should be
   * changed to <tt>HREF='Blah.do?Page=1&amp;Sort=ASC'</tt>. This is commonly
   * seen in building query strings. (In JSTL, the c:url tag performs this task
   * automatically.)
   */
  static public String forHTMLTag(String aTagFragment) {
    final StringBuilder result = new StringBuilder();

    final StringCharacterIterator iterator = new StringCharacterIterator(aTagFragment);
    char character = iterator.current();
    while (character != CharacterIterator.DONE) {
      if (character == '<') {
        result.append("&lt;");
      } else if (character == '>') {
        result.append("&gt;");
      } else if (character == '\"') {
        result.append("&quot;");
      } else if (character == '\'') {
        result.append("&#039;");
      } else if (character == '\\') {
        result.append("&#092;");
      } else if (character == '&') {
        result.append("&amp;");
      } else {
        //the char is not a special one
        //add it to the result as is
        result.append(character);
      }
      character = iterator.next();
    }
    return result.toString();
  }

  /**
   * Load properties file from classpath.
   */
  static public Properties loadPropertiesResource(String aResourcePath) throws IOException {
    try {
      // Use the class loader that loaded our class.
      // This is required where for reasons like security
      // multiple class loaders exist, e.g. BEA WebLogic.
      // Thanks to Lutz Lennemann 29-aug-2000.
      ClassLoader classLoader = Sys.class.getClassLoader();

      Properties properties = new Properties();

      // Try loading it.
      properties.load(classLoader.getResourceAsStream(aResourcePath));
      return properties;
    } catch (Throwable t) {
      throw new IOException("failed loading Properties resource from " + aResourcePath);
    }
  }

  /**
   * Load properties file from file path.
   */
  static public Properties loadPropertiesFile(String aFilePath) throws IOException {
    try {

      Properties properties = new Properties();

      // Try loading it.
      properties.load(new FileInputStream(aFilePath));
      return properties;
    } catch (Throwable t) {
      throw new IOException("failed loading Properties file from " + aFilePath);
    }
  }

  /**
   * Shorthand for current time.
   */
  static public long now() {
    return System.currentTimeMillis();
  }

  /**
   * Produce a string in double quotes with backslash sequences in all the right
   * places. A backslash will be inserted within </, allowing JSON text to be
   * delivered in HTML. In JSON text, a string cannot contain a control
   * character or an unescaped quote or backslash.
   * 
   * @param string
   *          A String
   * @return A String correctly formatted for insertion in a JSON text.
   */
  public static String quote(String string) {
    if (string == null || string.length() == 0) {
      return "\"\"";
    }

    char b;
    char c = 0;
    int i;
    int len = string.length();
    StringBuilder sb = new StringBuilder(len + 4);
    String t;

    sb.append('"');
    for (i = 0; i < len; i += 1) {
      b = c;
      c = string.charAt(i);
      switch (c) {
      case '\\':
      case '"':
        sb.append('\\');
        sb.append(c);
        break;
      case '/':
        if (b == '<') {
          sb.append('\\');
        }
        sb.append(c);
        break;
      case '\b':
        sb.append("\\b");
        break;
      case '\t':
        sb.append("\\t");
        break;
      case '\n':
        sb.append("\\n");
        break;
      case '\f':
        sb.append("\\f");
        break;
      case '\r':
        sb.append("\\r");
        break;
      default:
        if (c < ' ' || (c >= '\u0080' && c < '\u00a0') ||
                         (c >= '\u2000' && c < '\u2100')) {
          t = "000" + Integer.toHexString(c);
          sb.append("\\u" + t.substring(t.length() - 4));
        } else {
          sb.append(c);
        }
      }
    }
    sb.append('"');
    return sb.toString();
  }

}
