package com.google.cloud.training.dataanalyst.javahelp;
import java.time.Instant;
import java.util.ArrayList;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableSchema;

public class kafkatobq {

	public static interface MyOptions extends DataflowPipelineOptions {
	@Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
		@Default.String("cloud-training-demos:demos.streamdemo2")
		String getOutput();
		void setOutput(String s);
		@Description("Input topic")
		@Default.String("projects/cloud-training-demos/topics/streamdemo")
		String getInput();
		void setInput(String s);
	}
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);
		String topic = options.getInput();
		String output = options.getOutput();
		Map<String, Object> propertyBuilder = new HashMap();
        propertyBuilder.put("request.timeout.ms",20000);
        propertyBuilder.put("retry.backoff.ms",500);
		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("NAME").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ADDRESS").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ACC_NO").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);
		p //
				.apply(KafkaIO.<Long, String>read()
    			        	.withBootstrapServers("35.228.77.245:9092")
      			       		.withTopic("users")  // use withTopics(List<String>) to read from multiple topics.
      			        	.withKeyDeserializer(LongDeserializer.class)
    			        	.withValueDeserializer(StringDeserializer.class)
					.withoutMetadata()
				     )
				.apply(Values.<String>create())
				.apply("MapToTableRow", ParDo.of(new DoFn<String, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) { 
						Gson gson = new GsonBuilder().create();
						HashMap<String, Object> parsedMap = gson.fromJson(c.element().toString(), HashMap.class);
						TableRow row = new TableRow();
						row.set("NAME", parsedMap.get("name").toString());
						row.set("ADDRESS", parsedMap.get("address").toString());
						row.set("ID", parsedMap.get("id").toString());
						row.set("ACC_NO", parsedMap.get("acc_no").toString());
						c.output(row);
						}
					}))
				.apply(BigQueryIO.writeTableRows().to(output)//
						.withSchema(schema)//
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		p.run();

	}

}
