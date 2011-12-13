// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.
package nl.justobjects.pushlet.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.util.Random;

/**
 * Randomizing routines.
 * 
 * @author Just van den Broecke
 * @version $Id: Rand.java,v 1.4 2007/12/07 12:57:40 justb Exp $
 */
public class Rand {
  private static char CHARS[] = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'r',
      's', 't', 'u', 'v', 'w', 'y', 'z' };
  private static char NON_VOWELS[] = { 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't',
      'v', 'w', 'z' };
  private static char VOWELS[] = { 'a', 'e', 'i', 'o', 'u', 'y' };
  private static Random random = new Random();

  public static char randomChar() {
    return CHARS[randomInt(0, CHARS.length - 1)];
  }

  public static char randomVowel() {
    return VOWELS[randomInt(0, VOWELS.length - 1)];
  }

  public static char randomNonVowel() {
    return NON_VOWELS[randomInt(0, NON_VOWELS.length - 1)];
  }

  public static File randomTempDir() throws Exception {
    File file = new File(System.getProperty("java.io.tmpdir") + File.separator + "oasetest" + File.separator
        + randomString(12));
    file.mkdirs();
    file.deleteOnExit();
    return file;
  }

  public static File randomTempFile() throws Exception {
    File file = new File(System.getProperty("java.io.tmpdir") + File.separator + "oase-" + randomString(6));
    file.createNewFile();
    file.deleteOnExit();
    return file;
  }

  public static File randomBinaryFile(int aSize) throws Exception {
    File file = randomTempFile();
    FileOutputStream fos = new FileOutputStream(file);
    fos.write(randomBytes(aSize));
    fos.close();
    return file;
  }

  public static File randomTextFile(int aSize) throws Exception {
    File file = randomTempFile();
    PrintWriter pw = new PrintWriter(new FileOutputStream(file));
    pw.write(randomString(aSize));
    pw.close();
    return file;
  }

  public static byte[] randomBytes(int aSize) {
    return randomBlob(aSize);
  }

  public static byte[] randomBlob(int aSize) {
    byte[] retval = new byte[aSize];
    for (int i = 0; i < retval.length; i++) {
      retval[i] = randomByte();
    }
    return retval;
  }

  public static byte randomByte() {
    return (byte) random.nextInt();
  }

  public static double randomDouble() {
    return random.nextLong();
  }

  public static int randomInt() {
    return random.nextInt();
  }

  public static int randomInt(int min, int max) {
    return (int) ((Math.random() * (double) (max + 1 - min)) + min);
  }

  public static long randomLong() {
    return random.nextLong();
  }

  public static long randomLong(long min, long max) {
    return (long) ((Math.random() * (double) (max + 1L - min)) + min);
  }

  public static String randomName(int aLength) {
    StringBuilder sb = new StringBuilder(aLength);
    for (int i = 0; i < aLength; i++) {
      sb.append(i % 2 == 0 ? randomNonVowel() : randomVowel());
    }
    return sb.toString();
  }

  public static void randomSleep(long min, long max) {
    try {
      Thread.sleep(randomLong(min, max));
    } catch (InterruptedException ie) {

    }
  }

  public static String randomString(int aLength) {
    StringBuilder sb = new StringBuilder(aLength);
    for (int i = 0; i < aLength; i++) {
      sb.append(randomChar());
    }
    return sb.toString();
  }

  public static String randomString() {
    return "" + randomLong();
  }

  /**
   * 把字符串转化成MD5
   * @param inStr 需转换的字符串
   * @return 转换成MD5后的输出字符串
   * @throws RuntimeException 当发生错误时会抛出RuntimeException
   */
  public static String getMD5ofStr(String inStr) {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] md5Bytes = md5.digest(inStr.getBytes());
      return ByteArraytoHexString(md5Bytes, 0, md5Bytes.length);
    } catch (Throwable thex) {
      throw new RuntimeException(thex);
    }
  }

  /**
   * 把字节数组转换成16进制表示的字符串
   * @param b 待转换的字节数组
   * @param offset 起始偏移量,已0开始
   * @param size 要转换的字节个数
   * @return 转换后的16进制表示的字符串
   */
  public static String ByteArraytoHexString(byte[] b, int offset, int size) {
    StringBuilder sb = new StringBuilder(2 * size);
    int b1, b2;

    for (int i = offset; i < size; i++) {
      b1 = (b[i] >>> 4) & 0X0F;
      sb.append(Integer.toHexString(b1).toUpperCase());
      b2 = b[i] & 0X0F;
      sb.append(Integer.toHexString(b2).toUpperCase());
    }

    return sb.toString();
  }
  
}

