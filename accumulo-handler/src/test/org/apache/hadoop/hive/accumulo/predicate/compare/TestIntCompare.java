package org.apache.hadoop.hive.accumulo.predicate.compare;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class TestIntCompare {
  private IntCompare intCompare;

  @Before
  public void setup() {
    byte[] ibytes = new byte[4];
    ByteBuffer.wrap(ibytes).putInt(10);
    intCompare = new IntCompare();
    intCompare.init(ibytes);
  }

  public byte[] getBytes(int val) {
    byte[] intBytes = new byte[4];
    ByteBuffer.wrap(intBytes).putInt(val);
    int serializedVal = intCompare.serialize(intBytes);
    assertEquals(serializedVal, val);
    return intBytes;
  }

  @Test
  public void equal() {
    Equal equalObj = new Equal(intCompare);
    byte[] val = getBytes(10);
    assertTrue(equalObj.accept(val));
  }

  @Test
  public void notEqual() {
    NotEqual notEqualObj = new NotEqual(intCompare);
    byte[] val = getBytes(11);
    assertTrue(notEqualObj.accept(val));

    val = getBytes(10);
    assertFalse(notEqualObj.accept(val));

  }

  @Test
  public void greaterThan() {
    GreaterThan greaterThanObj = new GreaterThan(intCompare);
    byte[] val = getBytes(11);

    assertTrue(greaterThanObj.accept(val));

    val = getBytes(4);
    assertFalse(greaterThanObj.accept(val));

    val = getBytes(10);
    assertFalse(greaterThanObj.accept(val));
  }

  @Test
  public void greaterThanOrEqual() {
    GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(intCompare);

    byte[] val = getBytes(11);

    assertTrue(greaterThanOrEqualObj.accept(val));

    val = getBytes(4);
    assertFalse(greaterThanOrEqualObj.accept(val));

    val = getBytes(10);
    assertTrue(greaterThanOrEqualObj.accept(val));
  }

  @Test
  public void lessThan() {

    LessThan lessThanObj = new LessThan(intCompare);

    byte[] val = getBytes(11);

    assertFalse(lessThanObj.accept(val));

    val = getBytes(4);
    assertTrue(lessThanObj.accept(val));

    val = getBytes(10);
    assertFalse(lessThanObj.accept(val));

  }

  @Test
  public void lessThanOrEqual() {

    LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(intCompare);

    byte[] val = getBytes(11);

    assertFalse(lessThanOrEqualObj.accept(val));

    val = getBytes(4);
    assertTrue(lessThanOrEqualObj.accept(val));

    val = getBytes(10);
    assertTrue(lessThanOrEqualObj.accept(val));
  }

  @Test
  public void like() {
    try {
      Like likeObj = new Like(intCompare);
      assertTrue(likeObj.accept(new byte[] {}));
      fail("should not accept");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains(
          "Like not supported for " + intCompare.getClass().getName()));
    }
  }
}
