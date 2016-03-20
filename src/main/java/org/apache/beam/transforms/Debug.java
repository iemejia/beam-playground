package org.apache.beam.transforms;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;

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
