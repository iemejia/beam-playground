package org.apache.beam.contrib.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Arrays;
import java.util.List;

public class DebugTest {
  public static void main(final String... args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
//    options.setRunner(SparkPipelineRunner.class);
//    options.setProject("SET_YOUR_PROJECT_ID_HERE");
    Pipeline p = Pipeline.create(options);

    final List<String> LINES = Arrays.asList(
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ");

    p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
//        .apply(Log.print())
        .apply(FlatMapElements.via((String text) -> Arrays.asList(text.split(" ")))
            .withOutputType(new TypeDescriptor<String>() {
            }))
//        .apply(Log.print())
        .apply(Filter.by((String text) -> text.length() > 5))
//        .apply(Log.print())
        .apply(MapElements.via((String text) -> text.toUpperCase())
            .withOutputType(new TypeDescriptor<String>() {
            }));
//        .apply(Debug
//            .with((String s) -> {
//              System.out.println(s);
//              return null;
//            }))
//        .apply(Debug
//            .when((String s) -> s.startsWith("A"))
//            .with((String s) -> {
//              System.out.println(s);
//              return null;
//            }));
    p.run();
  }
}
