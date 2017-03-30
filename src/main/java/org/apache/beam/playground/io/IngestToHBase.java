package org.apache.beam.playground.io;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple pipeline to test read from HBase.
 */
public class IngestToHBase {

    private static final Logger LOG = LoggerFactory.getLogger(IngestToHBase.class);
    /**
     * Specific pipeline options.
     */
    public interface Options extends PipelineOptions {
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);

        final Configuration conf = HBaseConfiguration.create();
        String tableId = "test";

//        pipeline.apply("single row", Create.of(makeWrite(key, value)).withCoder(HBaseIO.WRITE_CODER))
//            .apply("write", HBaseIO.write().withConfiguration(conf).withTableId(table));
        final String key = "key";
        final String value = "value";


//        pipeline
//                .apply("SomeData", CountingInput.upTo(1 * 1024))
//                .apply("ConvertToKV", MapElements.via(
//                        new SimpleFunction<Long, KV<ByteString, Iterable<Mutation>>>() {
//                    @Override
//                    public KV<ByteString, Iterable<Mutation>> apply(Long input) {
//                        // let's assume key = value
//                        List<Mutation> value = new ArrayList<>();
//                        String prefix = String.format("%X", input % 16);
//                        byte[] rowKey =
//                            Bytes.toBytes(StringUtils.leftPad("_" + String.valueOf(input),
//                                    21, prefix));
//                        Put p = new Put(rowKey);
//                        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
//                                Bytes.toBytes(String.valueOf(input)));
//                        byte[] valueEmail;
//                        if  (input % 2 == 0) {
//                            valueEmail = Bytes.toBytes(String.valueOf(input) + "@email.com");
//                        } else {
//                            valueEmail = Bytes.toBytes(String.valueOf(input) + "@email.com CON MUCHISIMA MAS MIERDA Y MAS GRANDE ESTO TIENE QUE SER MAS GRANDE O SINO ME VOY A VOLVER LOCO");
//                        }
//                        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"),
//                                valueEmail);
//                        value.add(p);
//
//                        ByteString keys = ByteString.copyFromUtf8(input.toString());
//                        return KV.of(keys, (Iterable<Mutation>) value);
//                    }
//                }))
//                .setCoder(HBaseIO.WRITE_CODER)
//                .apply("WriteToHBase",
//                        HBaseIO.write().withConfiguration(conf).withTableId(tableId));
//        pipeline.run().waitUntilFinish();
    }
}

