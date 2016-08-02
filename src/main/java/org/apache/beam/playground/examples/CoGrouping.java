package org.apache.beam.playground.examples;

import com.google.common.collect.Multimap;

import org.apache.beam.contrib.transforms.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CoGrouping {
    public static void main(String[] args) {
        Pipeline p = TestPipeline.create();

//        PCollection<String> input = p.apply(Create.of("1", "2"));

//        PCollection<KV<String, Doc>> urlDocPairs = ...;
//        PCollection<KV<String, Iterable<Doc>>> urlToDocs =
//                urlDocPairs.apply(GroupByKey.<String, Doc>create());
//        PCollection<R> results =
//                urlToDocs.apply(ParDo.of(new DoFn<KV<String, Iterable<Doc>>, R>() {
//                    public void processElement(ProcessContext c) {
//                        String url = c.element().getKey();
//                        Iterable<Doc> docsWithThatUrl = c.element().getValue();
//         ... process all docs having that url ...
//                    }}));

//        Map<String, Long> map = new Multimap<>();
        Collection<KV<String, Integer>> numbers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            numbers.add(KV.of("number", i));
            if (i % 2 == 0) {
                numbers.add(KV.of("even", i));
            }
        }
        PCollection<KV<String, Integer>> kvInput = p.apply("firstvalue", Create.of(numbers));
        kvInput
//            .apply("Sample", Sample.any(5))
            .apply("GroupBy", GroupByKey.create())
            .apply("ExtractValues", Values.create())
            .apply("toString", ParDo.of(new DoFn<Iterable<Integer>, String>() {
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        c.output(c.element().toString());
                    }
                }))
//            .apply("toString", MapElements.via(new SimpleFunction<Integer, String>() {
//                @Override
//                public String apply(Integer inputT) {
//                    return inputT.toString();
//                }
//            })
//            .apply(Log.print())
            .apply(TextIO.Write.to("/tmp/beam-playground/cogrouping"));
//
//
//        PCollection<String> pt1 = p.apply(Create.of("one","two","three"));
//        PCollection<Integer> pt2 = p.apply(Create.of(1, 2, 3));
//
//        final TupleTag<String> t1 = new TupleTag<>();
//        final TupleTag<String> t2 = new TupleTag<>();
//
////        final TupleTag<String> eventInfoTag = new TupleTag<>();
////        final TupleTag<String> countryInfoTag = new TupleTag<>();
//
//        // transform both input collections to tuple collections, where the keys are country
//        // codes in both cases.
//        PCollection<KV<String, String>> eventInfo = eventsTable.apply(
//                ParDo.of(new ExtractEventDataFn()));
//        PCollection<KV<String, String>> countryInfo = countryCodes.apply(
//                ParDo.of(new ExtractCountryInfoFn()));
//
//        // country code 'key' -> CGBKR (<event info>, <country name>)
//        PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
//                .of(eventInfoTag, eventInfo)
//                .and(countryInfoTag, countryInfo)
//                .apply(CoGroupByKey.<String>create());
//
//
//        PCollection<KV<String, String>> eventInfo = eventsTable.apply(
//                ParDo.of(new ExtractEventDataFn()));
//
//        final PCollection<KV<K, CoGbkResult>> apply = KeyedPCollectionTuple.of(t1, pt1)
//                .and(t2, pt2)
//                .apply(CoGroupByKey.create());
//        p.run();
//
////        Create<KV<String, Long>>.of(map);
    }
}
