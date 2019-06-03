package com.google.cloud.training.dataanalyst.javahelp;
import java.time.Instant;
import java.util.ArrayList;
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
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class StreamDemoConsumer {

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

		// Build the table schema for the output table.

		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("NAME").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ADDRESS").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ACC_NO").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		KafkaIO.Read<byte[], String> kafkaIOReader = KafkaIO.read()
        .withBootstrapServers("192.168.99.100:32771")
        .withTopics(Arrays.asList("beam".split(",")))
        .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
        .withValueCoder(StringUtf8Coder.of());
		p //
				.apply("GetMessages", kafkaIOReader.withoutMetadata()) //
				.apply("window",
						Window.into(SlidingWindows//
								.of(Duration.standardMinutes(2))//
								.every(Duration.standardSeconds(30)))) //
				.apply("WordsPerLine", ParDo.of(new DoFn<String, Integer>() {
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						String line = c.element();
						c.output(line.split(" ").length);
					}
				}))//
					
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