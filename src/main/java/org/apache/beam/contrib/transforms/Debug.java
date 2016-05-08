package org.apache.beam.contrib.transforms;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

@Experimental
public final class Debug {

  private Debug() {

  }

  public static <T> TransformBuilder<T>
      when(final SerializableFunction<T, Boolean> predicate) {
    final TransformBuilder<T> ab = new TransformBuilder<T>();
    ab.predicate = predicate;
    return ab;
  }

  public static <T> PTransform<PCollection<T>, PCollection<T>>
      with(final SerializableFunction<T, Void> fun) {
    return new TransformBuilder<T>().with(fun);
  }

  private static class NoTransformWithEffect<T>
      extends PTransform<PCollection<T>, PCollection<T>> {

    private SerializableFunction<T, Void> fun;
    private SerializableFunction<T, Boolean> predicate = (T t) -> Boolean.TRUE;

    @Override
    public PCollection<T> apply(final PCollection<T> col) {
      col.apply(ParDo.named("Debug.Do")
          .of(new DoFn<T, T>() {
            @Override
            public void processElement(final ProcessContext c) {
              final T elem = c.element();
              // TODO make this more robust
              if (predicate.apply(elem)) {
                fun.apply(elem);
              }
              c.output(elem);
            }
          }));
      return col;
    }
  }

  public static class TransformBuilder<T> {
    private SerializableFunction<T, Boolean> predicate = (T t) -> Boolean.TRUE;

    public PTransform<PCollection<T>, PCollection<T>>
        with(final SerializableFunction<T, Void> fun) {
      final NoTransformWithEffect<T> t = new NoTransformWithEffect<T>();
      t.fun = fun;
      t.predicate = predicate;
      return t;
    }
  }

}
