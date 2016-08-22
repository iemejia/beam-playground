package org.apache.beam.contrib.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public class Trace {

    public static class Debug {

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
                extends PTransform<PCollection<T>, PCollection<T>> implements Serializable {

            private SerializableFunction<T, Void> fun;
            private SerializableFunction<T, Boolean> predicate = (T t) -> Boolean.TRUE;

            @Override
            public PCollection<T> apply(final PCollection<T> col) {
                col.apply("Debug.Do", ParDo.of(new DoFn<T, T>() {
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

        public static class TransformBuilder<T> implements Serializable {
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

    public static class Log {

        public static <T> PTransform<PCollection<T>, PCollection<T>> print() {
            return Debug.with((T t) -> {
                System.out.println(t.toString());
                return null;
            });
        }

        public static <T> PTransform<PCollection<T>, PCollection<T>> error() {
            return Debug.with((T t) -> {
                System.err.println(t.toString());
                return null;
            });
        }
    }

}
