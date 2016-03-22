package org.apache.beam.playground.examples;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by ismael on 2/25/16.
 */
public class GeoData {

  private int amount;
  private double longitude;
  private double latitude;

  @JsonCreator
  public GeoData(@JsonProperty("value") int amount,
                 double latitude,
                 double longitude) {
    this.amount = amount;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public double getLatitude() {
    return latitude;
  }

  @JsonIgnore // @JsonProperty if we would have the setter
  public double getLongitude() {
    return longitude;
  }

  //    @JsonProperty("value")
  public int getAmount() {
    return amount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    GeoData geoData = (GeoData) o;

    if (amount != geoData.amount)
      return false;
    if (Double.compare(geoData.longitude, longitude) != 0)
      return false;
    return Double.compare(geoData.latitude, latitude) == 0;

  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = amount;
    temp = Double.doubleToLongBits(longitude);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(latitude);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "GeoData{" +
        "amount=" + amount +
        ", longitude=" + longitude +
        ", latitude=" + latitude +
        '}';
  }
}
