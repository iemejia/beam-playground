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
            return ab;
        }

        public static <T> PTransform<PCollection<T>, PCollection<T>>
        with(final SerializableFunction<T, Void> fun) {
            return new TransformBuilder<T>().with(fun);
        }

        private static class NoTransformWithEffect<T>
                extends PTransform<PCollection<T>, PCollection<T>> implements Serializable {

            private SerializableFunction<T, Void> fun;
            private SerializableFunction<T, Boolean> predicate =
                    new SerializableFunction<T, Boolean>() {
                        @Override
                        public Boolean apply(T input) {
                            return Boolean.TRUE;
                        }
                    };


            @Override
            public PCollection<T> expand(final PCollection<T> col) {
                col.apply("Debug.Do", ParDo.of(new DoFn<T, T>() {
                    @ProcessElement
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
            public PTransform<PCollection<T>, PCollection<T>>
            with(final SerializableFunction<T, Void> fun) {
                final NoTransformWithEffect<T> t = new NoTransformWithEffect<T>();
                t.fun = fun;
                return t;
            }
        }

    }

    public static class Log {

        public static <T> PTransform<PCollection<T>, PCollection<T>> print() {
            return Debug.with(new SerializableFunction<T, Void>() {
                @Override
                public Void apply(T input) {
                    System.out.println(input.toString());
                    return null;
                }
            });
        }

        public static <T> PTransform<PCollection<T>, PCollection<T>> error() {
            return Debug.with(new SerializableFunction<T, Void>() {
                @Override
                public Void apply(T input) {
                    System.err.println(input.toString());
                    return null;
                }
            });
        }
    }

}
