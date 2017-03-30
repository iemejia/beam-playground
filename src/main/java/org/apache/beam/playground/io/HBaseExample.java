package org.apache.beam.playground.io;

import org.apache.beam.playground.examples.CountWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseExample {
  private static final Logger LOG = LoggerFactory.getLogger(CountWords.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    LOG.info(options.toString());

    Pipeline p = Pipeline.create(options);

    Configuration configuration = HBaseConfiguration.create();
    final String HBASE_TABLE = "test";
    p.apply("read", HBaseIO.read().withConfiguration(configuration).withTableId(HBASE_TABLE))
        .apply(ParDo.of(new DoFn<Result, String>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            Result result = c.element();
            String rowkey = Bytes.toString(result.getRow());
            System.out.println("row key: " + rowkey);
            c.output(rowkey);
          }
        }))
        .apply(Count.<String>globally())
        .apply("FormatResults", MapElements.via(new SimpleFunction<Long, String>() {
          public String apply(Long element) {
            System.out.println("result: " + element.toString());
            return element.toString();
          }
        }));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
