package io.enjapan.il.ginko;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static it.ozimov.cirneco.hamcrest.java7.base.IsBetween.between;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by dumoulma on 1/25/16.
 */
public class CompanySimilarityTest {

  private TupleFactory tupleFactory = TupleFactory.getInstance();
  CompanySimilarity udf = new CompanySimilarity();

  @Test
  public void callExecFromValidTuplesAsInputShouldReturnScore() throws IOException {
    Tuple c1 = tupleFactory.newTuple(
        Arrays.asList("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                      "123456", "www.url.com", 0, 30, 1000000));
    Tuple c2 = tupleFactory
        .newTuple(Arrays.asList("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                                "1-2-3", "123456", "url.com", 0, 30, 1000000));
    Tuple input = tupleFactory.newTuple(Arrays.asList(c1, c2));

    double score = udf.exec(input);

    assertThat(score, between(0.0, 100.0));
  }

  @Test
  public void callSimScoreFromValidObjectsShouldReturnScore() throws Exception {
    CompanyRecord a =
        CompanyRecord
            .create("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                    "123456", "www.url.com", true, 30, 1000000);
    CompanyRecord b =
        CompanyRecord.create("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                             "1-2-3", "123456", "url.com", true, 30, 1000000);


    double score = udf.simScore(a, b);
    assertThat(score, between(0.0, 100.0));
  }

  @Test
  public void testSimScore_isListed_null() throws Exception {
    Tuple c1 = tupleFactory.newTuple(
        Arrays.asList("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                      "123456", "www.url.com", null, 30, 1000000));
    Tuple c2 = tupleFactory
        .newTuple(Arrays.asList("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                                "1-2-3", "123456", "url.com", 0, 30, 1000000));
    Tuple input = tupleFactory.newTuple(Arrays.asList(c1, c2));

    double score = udf.exec(input);

    assertThat(score, between(0.0, 100.0));
  }

  @Test
  public void testSimScore_CompanyAge_null() throws Exception {
    Tuple c1 = tupleFactory.newTuple(
        Arrays.asList("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                      "123456", "www.url.com", 0, null, 1000000));
    Tuple c2 = tupleFactory
        .newTuple(Arrays.asList("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                                "1-2-3", "123456", "url.com", 0, 30, 1000000));
    Tuple input = tupleFactory.newTuple(Arrays.asList(c1, c2));

    double score = udf.exec(input);

    assertThat(score, between(0.0, 100.0));
  }

  @Test
  public void testSimScore_Capital_null() throws Exception {
    Tuple c1 = tupleFactory.newTuple(
        Arrays.asList("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                      "123456", "www.url.com", 0, 25, null));
    Tuple c2 = tupleFactory
        .newTuple(Arrays.asList("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                                "1-2-3", "123456", "url.com", 0, 30, 1000000));
    Tuple input = tupleFactory.newTuple(Arrays.asList(c1, c2));

    double score = udf.exec(input);

    assertThat(score, between(0.0, 100.0));
  }

  @Test
  public void testRatio_for_field() throws Exception {

  }
}

