package io.enjapan.il.ginko;

import com.wantedtech.common.xpresso.strings.FuzzyWuzzy;
import datafu.pig.util.AliasableEvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Computes the similarity of two records on a 100 point scale.
 * <p>
 * Created by dumoulma on 1/25/16.
 */
public class CompanySimilarity extends AliasableEvalFunc<Double> {
  final Logger logger = LoggerFactory.getLogger(CompanySimilarity.class);

  @Override
  public Double exec(Tuple input) throws IOException {
    //    DataBag lbcRecordBag = getBag(input, "lbc");
    //    DataBag kestrelRecord = getBag(input, "kestrel");
    //
    //    Tuple lbcRecord = lbcRecordBag.iterator().next();
    //    Double interest = getDouble(interestTuple, getPrefixedAliasName("lbc", "interest_rate"));
    CompanyRecord a =
        CompanyRecord.create("MS", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku", "1-2-3",
                             "123456", true, 30, 1000000);
    CompanyRecord b =
        CompanyRecord.create("MSFT", "bill", "1234567", "Tokyo", "", "Shinjuku", "Shinjuku",
                             "1-2-3", "123456", true, 30, 1000000);
    return simScore(a, b);
  }

  @Override
  public Schema getOutputSchema(Schema schema) {
    return null;
  }

  double simScore(CompanyRecord a, CompanyRecord b) {
    double score = 0.0;
    score += stringRatio(a.name(), b.name()) * 5;
    score += stringRatio(a.repName(), b.repName()) * 1;
    score += stringRatio(a.zip(), b.zip()) * 3;
    score += stringRatio(a.tel(), b.tel()) * 3;
    score += stringRatio(a.pref(), b.pref());
    score += stringRatio(a.county(), b.county(), true);
    score += stringRatio(a.ward(), b.ward(), true);
    score += stringRatio(a.city(), b.city());
    score += stringRatio(a.address(), b.address());
    score += numericalRatio(a.age(), b.age());
    score += numericalRatio(a.capital(), b.capital());
    score += (a.isListed() ^ b.isListed() ? 0.0 : 1.0); // ^ is XOR
    score *= 5;
    logger.info("Score: {} Company a:{} b:{}", score, a, b);
    return score;
  }

  double numericalRatio(int a, int b) {
    if (b == 0)
      return 0.0;
    return Integer.min(a, b) / Integer.max(a, b);
  }

  double stringRatio(String x, String y) {
    return stringRatio(x, y, false);
  }

  double stringRatio(String x, String y, boolean x_y_is_zero) {
    if (x != null || x.isEmpty() || x.equalsIgnoreCase("NA")) {
      if (y != null || y.isEmpty() || y.equalsIgnoreCase("NA")) {
        return x_y_is_zero ? 0.0 : 1.0;
      }
      return 0.0;
    }
    return FuzzyWuzzy.ratio(x, y) * 0.01;
  }
}
