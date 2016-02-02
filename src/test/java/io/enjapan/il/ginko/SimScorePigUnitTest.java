package io.enjapan.il.ginko;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import java.io.File;

/**
 * Tests Simscore using PigUnit
 * Created by dumoulma on 2/2/16.
 */
public class SimScorePigUnitTest {
  @Test
  public void testTop2Queries() throws Exception {
    File currentDirFile = new File(".");
    String helper = currentDirFile.getAbsolutePath();
    String currentDirStr = helper.substring(0, helper.indexOf(".") - 1);

    String[] args = {
        "baseDir=" + currentDirStr,
    };

    PigTest test = new PigTest(currentDirStr + "/pig/test/simscore_pigunit.pig", args);

    String[] output = {
        "(135,135,25.0)",
        "(340,340,20.0)",
    };

    test.assertOutput("scored", output);
  }
}
