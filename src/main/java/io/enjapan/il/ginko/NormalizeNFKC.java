package io.enjapan.il.ginko;

import datafu.pig.util.SimpleEvalFunc;
import org.apache.pig.data.Tuple;
import java.text.Normalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mtauchi on 2/2/16.
 */
public class NormalizeNFKC extends SimpleEvalFunc<Tuple> {
  private static final Logger LOG = LoggerFactory.getLogger(TfIdfScore.class);

  public String call(Tuple inputStrTuple) {
    if (inputStrTuple == null || inputStrTuple.size() != 1){
  		return null;
  	}
    try{
      String inputStr = (String)inputStrTuple.get(0);
      if(inputStr == null){ return null; }
  	  String outputStr = Normalizer.normalize(inputStr, Normalizer.Form.NFKC);
      LOG.debug(String.format("inputStr: %s", inputStr));
  	  return outputStr;
    } catch (Exception e){
      return null;
    }
  }
}
