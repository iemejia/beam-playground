package org.apache.beam.contrib.io;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Transform for printing the contents of a {@link org.apache.beam.sdk.values.PCollection}.
 * to standard output.
 *
 * This is derived from the one in the Flink Runner
 */
public class ConsoleIO {

  /**
   * A PTransform that writes a PCollection to a standard output.
   */
  public static class Write {

    /**
     * Returns a ConsoleIO.Write PTransform with a default step name.
     */
    public static Bound create() {
      return new Bound();
    }

    /**
     * Returns a ConsoleIO.Write PTransform with the given step name.
     */
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * A PTransform that writes a bounded PCollection to standard output.
     */
    public static class Bound extends PTransform<PCollection<?>, PDone> {
      private static final long serialVersionUID = 0;

      Bound() {
        super("ConsoleIO.Write");
      }

      Bound(String name) {
        super(name);
      }

      /**
       * Returns a new ConsoleIO.Write PTransform that's like this one but with the given
       * step
       * name.  Does not modify this object.
       */
      public Bound named(String name) {
        return new Bound(name);
      }

      @Override
      public PDone expand(PCollection<?> input) {
//        return new PDone();
//        input.getPipeline().getOptions().
        return PDone.in(input.getPipeline());

      }
    }
  }
}


