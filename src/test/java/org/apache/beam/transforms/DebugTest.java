package org.apache.beam.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import java.util.Arrays;
import java.util.List;

public class DebugTest {
  public static void main(final String... args) {
    DataflowPipelineOptions options = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);
    options.setRunner(DirectPipelineRunner.class);
    options.setProject("SET_YOUR_PROJECT_ID_HERE");
    Pipeline p = Pipeline.create(options);

    final List<String> LINES = Arrays.asList(
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ");

    p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
        .apply(Log.print())
        .apply(FlatMapElements.via((String text) -> Arrays.asList(text.split(" ")))
            .withOutputType(new TypeDescriptor<String>() {
            }))
        .apply(Log.print())
        .apply(Filter.byPredicate((String text) -> text.length() > 5))
        .apply(Log.print())
        .apply(MapElements.via((String text) -> text.toUpperCase())
            .withOutputType(new TypeDescriptor<String>() {
            }))
        .apply(Debug
            .with((String s) -> {
              System.out.println(s);
              return null;
            }))
        .apply(Debug
            .when((String s) -> s.startsWith("A"))
            .with((String s) -> {
              System.out.println(s);
              return null;
            }));
    p.run();
  }
}
