package io.enjapan.il.ginko;

import datafu.pig.util.SimpleEvalFunc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * Created by dumoulma on 1/26/16.
 */
public class CompanyAge extends SimpleEvalFunc<Double> {
  private static final Logger LOG = LoggerFactory.getLogger(TfIdfScore.class);

  public Integer call(Integer setupYear) {
    int currentYear = LocalDateTime.now().getYear();
    int age = currentYear - setupYear.intValue();
    LOG.debug(String.format("setupYear: %d currentYear: %d age: %d", setupYear, currentYear, age));
    return age;
  }
}
