package com.gcp.poc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class gcppocjob {

	/**
	 * StringToRowConverterAd to convert the csv into TableRow
	 * 
	 * @author kosalan
	 */
	static class StringToRowConverterAd extends DoFn<String, TableRow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			String inputline = c.element();
			String[] split = inputline.split(",");
			TableRow outputrow = new TableRow();
			outputrow.set("AdUnitID", split[0]);
			outputrow.set("ContentID", split[1]);
			outputrow.set("Publisher", split[2]);
			c.output(outputrow);
		}
	}

	/**
	 * StringToRowConverterUser to convert the csv into TableRow
	 * 
	 * @author kosalan
	 */
	static class StringToRowConverterUser extends DoFn<String, TableRow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			String inputline = c.element();
			String[] split = inputline.split(",");
			TableRow outputrow = new TableRow();
			outputrow.set("UserID", split[0]);
			outputrow.set("OS", split[1]);
			outputrow.set("PostalCode", split[2]);
			c.output(outputrow);
		}
	}

	/**
	 * StringToRowConverterImpression to convert the csv into TableRow
	 * 
	 * @author kosalan
	 */
	static class StringToRowConverterImpression extends DoFn<String, TableRow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			String inputline = c.element();
			String[] split = inputline.split(",");
			TableRow outputrow = new TableRow();
			outputrow.set("AdUnitID", split[0]);
			outputrow.set("UserID", split[1]);
			outputrow.set("ClientID", split[2]);
			outputrow.set("GfpActivityAdEventTIme", split[3]);
			c.output(outputrow);
		}
	}

	/**
	 * StringToRowConverterFirstJoin to convert the String into TableRow
	 * 
	 * @author kosalan
	 */
	static class StringToRowConverterFirstJoin extends DoFn<String, TableRow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			String inputline = c.element();
			String[] split = inputline.split("%");
			TableRow outputrow = new TableRow();
			outputrow.set("AdUnitID", split[0]);
			outputrow.set("UserID", split[1]);
			outputrow.set("ClientID", split[2]);
			outputrow.set("GfpActivityAdEventTIme", split[3]);
			outputrow.set("ContentID", split[4]);
			outputrow.set("Publisher", split[5]);
			c.output(outputrow);
		}
	}

	/**
	 * StringToRowConverterSecondJoin to convert the String into TableRow
	 * 
	 * @author kosalan
	 */
	static class StringToRowConverterSecondJoin extends DoFn<String, TableRow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			String inputline = c.element();
			String[] split = inputline.split("%");
			TableRow outputrow = new TableRow();
			outputrow.set("UserID", split[0]);
			outputrow.set("AdUnitID", split[1]);
			outputrow.set("ClientID", split[2]);
			outputrow.set("GfpActivityAdEventTIme", split[3]);
			outputrow.set("ContentID", split[4]);
			outputrow.set("Publisher", split[5]);
			outputrow.set("OS", split[6]);
			outputrow.set("PostalCode", split[7]);
			c.output(outputrow);
		}
	}

	/**
	 * Examines each row (ImpressionTable) in the input table. Output a KV with
	 * the key the AdUnitID code of the Impression, and the value a string
	 * encoding Impression information.
	 */
	static class ExtractImpressionDataInfoFn extends DoFn<TableRow, KV<String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String Ad_ID = (String) row.get("AdUnitID");
			String User_ID = (String) row.get("UserID");
			String Client_ID = (String) row.get("ClientID");
			String Impr_Time = (String) row.get("GfpActivityAdEventTIme");
			// String ImprInfo = "UserID: " + User_ID + " ClientID: " +
			// Client_ID + " GfpActivityAdEventTIme: " + Impr_Time;
			String ImprInfo = User_ID + "%" + Client_ID + "%" + Impr_Time;
			c.output(KV.of(Ad_ID, ImprInfo));
		}
	}

	/**
	 * Examines each row (AdTable) in the input table. Output a KV with the key
	 * the AdUnit code, and the value the AdInfo name.
	 */
	static class ExtractAdDataInfoFn extends DoFn<TableRow, KV<String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String Ad_ID = (String) row.get("AdUnitID");
			String Content_ID = (String) row.get("ContentID");
			String Pub_ID = (String) row.get("Publisher");
			// String Add_Info = "ContentID: " + Content_ID + " Publisher: " +
			// Pub_ID;
			String Add_Info = Content_ID + "%" + Pub_ID;
			c.output(KV.of(Ad_ID, Add_Info));
		}
	}

	/**
	 * Examines each row (UserTable) in the input table. Output a KV with the
	 * key the UserID code, and the value the UserInfo name.
	 */
	static class ExtractUserDataInfoFn extends DoFn<TableRow, KV<String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String User_ID = (String) row.get("UserID");
			String OS_ID = (String) row.get("OS");
			String Post_ID = (String) row.get("PostalCode");
			// String User_Info = "OS: " + OS_ID + " PostalCode: " + Post_ID;
			String User_Info = OS_ID + "%" + Post_ID;
			c.output(KV.of(User_ID, User_Info));
		}
	}

	/**
	 * Examines each row (FirstJoin) in the input table. Output a KV with the
	 * key the UserID code, and the value the UserInfo name.
	 */
	static class ExtractFirstJoinDataInfoFn extends DoFn<TableRow, KV<String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String Ad_ID = (String) row.get("AdUnitID");
			String User_ID = (String) row.get("UserID");
			String Client_ID = (String) row.get("ClientID");
			String Impr_Time = (String) row.get("GfpActivityAdEventTIme");
			String Content_ID = (String) row.get("ContentID");
			String Pub = (String) row.get("Publisher");

			String Joint_Info = Ad_ID + "%" + Client_ID + "%" + Impr_Time + "%" + Content_ID + "%" + Pub;
			c.output(KV.of(User_ID, Joint_Info));
		}
	}

	/**
	 * Join two collections, using country code as the key.
	 */
	static PCollection<String> joinEvents(PCollection<TableRow> ImpressionTable, PCollection<TableRow> AdTable,
			PCollection<TableRow> UserTable) throws Exception {

		final TupleTag<String> ImpressionInfoTag = new TupleTag<String>();
		final TupleTag<String> AdInfoTag = new TupleTag<String>();
		final TupleTag<String> UserInfoTag = new TupleTag<String>();

		// transform both input collections to tuple collections, where the keys
		// are AdUnitID
		PCollection<KV<String, String>> ImpressionInfo = ImpressionTable
				.apply(ParDo.of(new ExtractImpressionDataInfoFn()));
		PCollection<KV<String, String>> AdInfo = AdTable.apply(ParDo.of(new ExtractAdDataInfoFn()));
		PCollection<KV<String, String>> UserInfo = UserTable.apply(ParDo.of(new ExtractUserDataInfoFn()));

		// country code 'key' -> CGBKR (<Impression info>, <Ad Info>)
		PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple.of(ImpressionInfoTag, ImpressionInfo)
				.and(AdInfoTag, AdInfo).apply(CoGroupByKey.<String>create());

		// Process the CoGbkResult elements generated by the CoGroupByKey
		// transform.
		// AdUnitID 'key' -> string of <Impression info>, <Ad Info>
		PCollection<KV<String, String>> finalResultCollection = kvpCollection
				.apply(ParDo.named("Process First Join (Ad/Impression)").of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(ProcessContext c) {
						KV<String, CoGbkResult> e = c.element();
						String Ad_ID = e.getKey();
						Iterable<String> Ad_Info = null;
						Ad_Info = e.getValue().getAll(AdInfoTag);
						for (String ImpressionInfo : c.element().getValue().getAll(ImpressionInfoTag)) {
							// Generate a string that combines information from
							// both collection values
							c.output(KV.of(Ad_ID, "%" + ImpressionInfo + "%" + Ad_Info));
						}
					}
				}));

		// Format the Results from join
		PCollection<String> formattedResults = finalResultCollection
				.apply(ParDo.named("Format the results from first join (Ad/Impression)").of(new DoFn<KV<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(ProcessContext c) {

						String outputstring = c.element().getKey() + c.element().getValue();

						int jointbegin = outputstring.indexOf("[");
						String firsthalf = outputstring.substring(0, jointbegin);
						String secondhalf = outputstring.substring(outputstring.indexOf("[") + 1,
								outputstring.indexOf("]"));

						if (!secondhalf.isEmpty()) {
							String[] ad_data = secondhalf.split(",");

							for (int i = 0; i < ad_data.length; i++) {
								String final_string = firsthalf + ad_data[i];
								c.output(final_string);
							}
						}
					}
				}));

		PCollection<TableRow> FirstJointTable = formattedResults.apply("Changing First Join String to TableRow",
				ParDo.of(new StringToRowConverterFirstJoin()));
		final TupleTag<String> FirstJointInfoTag = new TupleTag<String>();
		PCollection<KV<String, String>> FirstJoinInfo = FirstJointTable
				.apply(ParDo.of(new ExtractFirstJoinDataInfoFn()));

		PCollection<KV<String, CoGbkResult>> kvpCollection2 = KeyedPCollectionTuple.of(FirstJointInfoTag, FirstJoinInfo)
				.and(UserInfoTag, UserInfo).apply(CoGroupByKey.<String>create());

		PCollection<KV<String, String>> finalResultCollection2 = kvpCollection2
				.apply(ParDo.named("Process Second Join (First Join Result / User Table").of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(ProcessContext c) {
						KV<String, CoGbkResult> e = c.element();
						String User_ID = e.getKey();
						Iterable<String> User_Info = null;
						User_Info = e.getValue().getAll(UserInfoTag);
						for (String FirstJointInfo : c.element().getValue().getAll(FirstJointInfoTag)) {
							// Generate a string that combines information from
							// both collection values
							c.output(KV.of(User_ID, "%" + FirstJointInfo + "%" + User_Info));
						}
					}
				}));

		// write to GCS
		PCollection<String> formattedResults2 = finalResultCollection2
				.apply(ParDo.named("Format Final Joint").of(new DoFn<KV<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(ProcessContext c) {

						String outputstring2 = c.element().getKey() + c.element().getValue();

						int jointbegin = outputstring2.indexOf("[");
						String firsthalf = outputstring2.substring(0, jointbegin);
						String secondhalf = outputstring2.substring(outputstring2.indexOf("[") + 1,
								outputstring2.indexOf("]"));

						if (!secondhalf.isEmpty()) {
							String[] user_data = secondhalf.split(",");

							for (int i = 0; i < user_data.length; i++) {
								String final_string = firsthalf + user_data[i];
								c.output(final_string);
							}
						}
					}
				}));

		return formattedResults2;
	}

	/**
	 * Main Method
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Pipeline Options Configuration
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("gcpbigdatapoc");
		options.setStagingLocation("gs://gcppocbucket/staging/");
		options.setNumWorkers(15);

		// Creating a Pipeline
		Pipeline p = Pipeline.create(options);

		// Loading all the csvs to pipeline
		PCollection<String> Ad_line = p.apply(TextIO.Read
				.from("gs://gcppocbucket/cloudstorage/20170929/Advertisement.csv").named("Load Advertisement.csv"));
		PCollection<String> Users_line = p
				.apply(TextIO.Read.from("gs://gcppocbucket/cloudstorage/20170929/Users.csv").named("Load Users.csv"));
		PCollection<String> Impression_line = p.apply(TextIO.Read
				.from("gs://gcppocbucket/cloudstorage/20170929/Impression.csv").named("Load Impression.csv"));

		// Transforming the PCollection into TableRow
		PCollection<TableRow> Ad_row = Ad_line.apply("Changing Ad_Line to Ad_row",
				ParDo.of(new StringToRowConverterAd()).named("Convert Advertisement.csv to Ad Table"));
		PCollection<TableRow> User_row = Users_line.apply("Changing Users_Line to Users_row",
				ParDo.of(new StringToRowConverterUser()).named("Changing Users.csv to Users Table"));
		PCollection<TableRow> Impression_row = Impression_line.apply("Changing Impression_Line to Impression_row",
				ParDo.of(new StringToRowConverterImpression()).named("Changing Impression.csv to Impression Table"));

		// Loading the PCollection to BigQuery Table
		// Ad_row.apply(BigQueryIO.Write.named("Writing to
		// Bigquery").to("gcpbigdatapoc:gcppocdataset.Advertisement").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		// User_row.apply(BigQueryIO.Write.named("Writing to
		// Bigquery").to("gcpbigdatapoc:gcppocdataset.Users").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		// Impression_row.apply(BigQueryIO.Write.named("Writing to
		// Bigquery").to("gcpbigdatapoc:gcppocdataset.Impression").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

		// Load BigQuery Table into New PCollection
		// PCollection<TableRow> AdTable =
		// p.apply(BigQueryIO.Read.from("gcpbigdatapoc:gcppocdataset.Advertisement"));
		// PCollection<TableRow> UserTable =
		// p.apply(BigQueryIO.Read.from("gcpbigdatapoc:gcppocdataset.Users"));
		// PCollection<TableRow> ImpressionTable =
		// p.apply(BigQueryIO.Read.from("gcpbigdatapoc:gcppocdataset.Impression"));

		// PCollection<String> formattedResults = joinEvents(ImpressionTable,
		// AdTable);
		PCollection<String> formattedResults = joinEvents(Impression_row, Ad_row, User_row);
		//formattedResults.apply(TextIO.Write.to("gs://gcppocbucket/cloudstorage/output.txt").withoutSharding());
		PCollection<TableRow> secondjoint = formattedResults.apply("Changing Second Join String to TableRow",
				ParDo.of(new StringToRowConverterSecondJoin()));
		secondjoint
				.apply(BigQueryIO.Write.named("Write Final Results to BQ").to("gcpbigdatapoc:gcppocdataset.doubleclickdata_warehouse")
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		// PCollection<TableRow> firstjoint = formattedResults.apply("Changing
		// First Join String to TableRow", ParDo.of(new
		// StringToRowConverterFirstJoin()));
		// firstjoint.apply(BigQueryIO.Write.named("Write to
		// BQ").to("gcpbigdatapoc:gcppocdataset.firstjoint").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

		p.run();

	}

}
