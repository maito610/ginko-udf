package io.enjapan.il.ginko;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class CompanyRecord {
  public abstract String name();

  public abstract String repName();

  public abstract String tel();

  public abstract String pref();

  public abstract String county();

  public abstract String ward();

  public abstract String city();

  public abstract String address();

  public abstract String zip();

  public abstract String url();

  public abstract boolean isListed();

  public abstract int age();

  public abstract int capital();

  public static CompanyRecord create(String n, String r, String tel, String p, String county,
                                     String w, String city, String a, String zip, String url,
                                     boolean l, int age, int capital) {
    return new AutoValue_CompanyRecord(n, r, tel, p, county, w, city, a, zip, url, l, age, capital);
  }
}
