package org.apache.beam.playground.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.apache.beam.contrib.io.ConsoleIO;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ismael on 3/17/16.
 */
public class CountWords {
  public static void main(String[] args) {
    DataflowPipelineOptions options = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);
    options.setRunner(DirectPipelineRunner.class);
    options.setProject("SET_YOUR_PROJECT_ID_HERE");
    Pipeline p = Pipeline.create(options);

    // Create a Java Collection, in this case a List of Strings.
    final List<String> LINES = Arrays.asList(
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ");

//        PCollection col = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
    p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
        .apply(FlatMapElements.via((String text) -> Arrays.asList(text.split(" ")))
            .withOutputType(new TypeDescriptor<String>() {
            }))
        .apply(MapElements.via((String text) -> text.toUpperCase())
            .withOutputType(new TypeDescriptor<String>() {
            }))
//                .apply("upper", MapElements.via(new SimpleFunction<String, String>() {
//                    @Override
//                    public String apply(String input) {
//                        return input.toUpperCase();
//                    }
//                }))
//        .apply(TextIO.Write.to("/tmp/petitprince-out.txt"));
        .apply(ConsoleIO.Write.create());

    p.run();

//    Pipeline q = Pipeline.create(options);
//    q.apply(TextIO.Read.from("/home/ismael/Downloads/petitprince.txt"))
//        .apply(ParDo.named("ExtractWords").of(new DoFn<String, String>() {
//          @Override
//          public void processElement(ProcessContext c) {
//            for (String word : c.element().split("[^a-zA-Z']+")) {
//              if (!word.isEmpty()) {
//                c.output(word);
//              }
//            }
//          }
//        }))
//        .apply(Count.<String>perElement())
//        .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//          @Override
//          public String apply(KV<String, Long> input) {
//            return input.getKey() + ": " + input.getValue();
//          }
//        }))
//        .apply(TextIO.Write.to("/tmp/petitprince-out.txt"));
//    q.run();
//        Debug.apply(null);

  }
}
