package org.apache.beam.contrib.transforms;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;

public final class Log {

  public static <T> PTransform<PCollection<T>, PCollection<T>> print() {
    return Debug.with((T t) -> { System.out.println(t); return null; });
  }

}
