package io.enjapan.il.ginko;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by dumoulma on 1/25/16.
 */
public class CompanySimilarityTest {

  private TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void testIdentityUDF() throws IOException {

    Tuple input = tupleFactory.newTuple(Arrays.asList("a", "b", "c"));
    IdentityUDF func = new IdentityUDF();
    Tuple output = func.exec(input);
    Assert.assertEquals(input, output);
  }

  @Test
  public void testSimScore() throws Exception {

  }

  @Test
  public void testRatio_for_field() throws Exception {

  }
}

//CompanyRecord a =
//    CompanyRecord.create("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
//                         "123456", true, 30, 1000000);
//CompanyRecord b =
//    CompanyRecord.create("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
//                         "1-2-3", "123456", true, 30, 1000000);
