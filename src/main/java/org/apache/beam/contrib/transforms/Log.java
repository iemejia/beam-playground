package org.apache.beam.contrib.transforms;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.io.PrintStream;

public final class Log {

  public static <T> PTransform<PCollection<T>, PCollection<T>> using(PrintStream printStream) {
    return Debug.with((T t) -> {
      printStream.println();
      return null;
    });
  }

  public static <T> PTransform<PCollection<T>, PCollection<T>> print() {
    return using(System.out);
  }

  public static <T> PTransform<PCollection<T>, PCollection<T>> error() {
    return using(System.err);
  }
}
