package io.enjapan.il.ginko;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Created by dumoulma on 1/25/16.
 */
public class CompanySimilarityTest {

  private TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void testIdentityUDF() throws IOException {

    //    Tuple input = tupleFactory.newTuple(Arrays.asList("a", "b", "c"));
    //    IdentityUDF func = new IdentityUDF();
    //    Tuple output = func.exec(input);
    //    Assert.assertEquals(input, output);
  }

  @Test
  public void testSimScore() throws Exception {
    CompanyRecord a =
        CompanyRecord
            .create(1, "MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                    "123456", "www.url.com", true, 30, 1000000);
    CompanyRecord b =
        CompanyRecord.create(2, "MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                             "1-2-3", "123456", "url.com", true, 30, 1000000);

    CompanySimilarity udf = new CompanySimilarity();
    double score = udf.simScore(a, b);
    assertThat(score, greaterThan(0.0));
  }

  @Test
  public void testRatio_for_field() throws Exception {

  }
}

