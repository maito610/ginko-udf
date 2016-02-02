package io.enjapan.il.ginko;

import datafu.pig.util.SimpleEvalFunc;
import java.text.Normalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mtauchi on 2/2/16. 
 */
public class NormalizeNFKC extends SimpleEvalFunc<String> {
  private static final Logger LOG = LoggerFactory.getLogger(TfIdfScore.class);

  public String call(String inputStr) {
    String outputStr = Normalizer.normalize(inputStr, Normalizer.Form.NFKC);
    LOG.debug(String.format("inputStr: %s", inputStr));
    return outputStr;
  }
}
