package io.enjapan.il.ginko;

import com.google.common.base.Strings;
import com.wantedtech.common.xpresso.strings.FuzzyWuzzy;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Computes the similarity of two records on a 100 point scale.
 * <p>
 * Created by dumoulma on 1/25/16.
 */
public class CompanySimilarity extends EvalFunc<Double> {
  final Logger logger = LoggerFactory.getLogger(CompanySimilarity.class);

  @Override
  public Double exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return 0.0;
    if (input.size() != 2) {
      throw new PigException("Input size must be two! got: " + input.size(),
                             new IllegalArgumentException());
    }
    try {
      Tuple left = (Tuple) input.get(0);
      Tuple right = (Tuple) input.get(1);

      CompanyRecord c1 = tupleToCompany(left);
      CompanyRecord c2 = tupleToCompany(right);
      double score = simScore(c1, c2);
      logger.debug("Sim score:{} c1:{} c2:{}", score, c1, c2);
      return score;
    } catch (Exception e) {
      logger.error("Exception thrown while extracting data from input Tuple! \n{}", e.getMessage());
      return null;
    }
  }

  private CompanyRecord tupleToCompany(Tuple tup) throws ExecException {
    try {
      String n = Strings.nullToEmpty((String) tup.get(0));
      String rn = Strings.nullToEmpty((String) tup.get(1));
      String t = Strings.nullToEmpty((String) tup.get(2));
      String p = Strings.nullToEmpty((String) tup.get(3));
      String county = Strings.nullToEmpty((String) tup.get(4));
      String w = Strings.nullToEmpty((String) tup.get(5));
      String city = Strings.nullToEmpty((String) tup.get(6));
      String a = Strings.nullToEmpty((String) tup.get(7));
      String z = Strings.nullToEmpty((String) tup.get(8));
      String u = Strings.nullToEmpty((String) tup.get(9));
      Integer isL = (Integer) tup.get(10);
      Integer age = (Integer) tup.get(11);
      Integer cap = (Integer) tup.get(12);
      CompanyRecord c =
          CompanyRecord.create(n, rn, t, p, county, w, city, a, z, u, isL != 0, age, cap);
      logger.debug("Got: {}", c);
      return c;
    } catch (NumberFormatException nfe) {
      logger.warn("NFE thrown! {} {}", nfe.getCause(), nfe.getMessage(), nfe);
    } catch (Exception e) {
      logger.warn("Exception thrown! {} {}", e.getCause(), e.getMessage(), e);
    }
    return CompanyRecord.create("", "", "", "", "", "", "", "", "", "", false, 0, 0);
  }

  //  @Override
  //  public Schema getOutputSchema(Schema schema) {
  //    return null;
  //  }

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
    score += stringRatio(a.url(), b.url());
    score += numericalRatio(a.age(), b.age());
    score += numericalRatio(a.capital(), b.capital());
    score += (a.isListed() ^ b.isListed() ? 0.0 : 1.0); // ^ is XOR
    score *= 5;
    logger.debug("Score: {} Company a:{} b:{}", score, a, b);
    if (score > 100.0)
      throw new ArithmeticException("Score is greater than 100!?");
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

  /**
   * Computes the fuzzy similarity of two strings as a ratio between 0 and  1;
   *
   * @param x
   * @param y
   * @param is_empty_match if two empty strings should match as 1 or 0
   * @return
   */
  double stringRatio(String x, String y, boolean is_empty_match) {
    if (x != null || x.isEmpty() || x.equalsIgnoreCase("NA")) {
      if (y != null || y.isEmpty() || y.equalsIgnoreCase("NA")) {
        return is_empty_match ? 1.0 : 0.0;
      }
      return 0.0;
    }
    return FuzzyWuzzy.ratio(x, y) * 0.01;
  }
}
