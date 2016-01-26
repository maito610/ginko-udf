package io.enjapan.il.ginko;

import datafu.pig.util.SimpleEvalFunc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dumoulma on 1/26/16.
 */
public class TfIdfScore extends SimpleEvalFunc<Double> {
  private static final Logger LOG = LoggerFactory.getLogger(TfIdfScore.class);

  public Double call(Long tfCount, Long nDocs, Long dfCount) {
    double tfidfScore = tfCount * Math.log(nDocs / (1.0 + dfCount));
    LOG.debug(String.format("tfCount: %d nDocs: %d dfCount: %d", tfCount, nDocs, dfCount));
    return tfidfScore;
  }
}
