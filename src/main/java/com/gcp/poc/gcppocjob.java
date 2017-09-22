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

public class gcppocjob 
{
	
	/**
	 * StringToRowConverterAd to convert the csv into TableRow
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
	 * Examines each row (ImpressionTable) in the input table. Output a KV with
	 * the key the UserID code of the Impression, and the value a string
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
		    String ImprInfo = "UserID: " + User_ID + ", ClientID: " + Client_ID + ", GfpActivityAdEventTIme: " + Impr_Time;
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
			String Add_Info = "ContentID: " + Content_ID + ", Publisher: " + Pub_ID;
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
			String User_Info = "OS: " + OS_ID + ", PostalCode: " + Post_ID;
			c.output(KV.of(User_ID, User_Info));
		}
	}
	
	  /**
	   * Join two collections, using country code as the key.
	   * eventsTable -- > ImpressionTable
	   * CountryCodes -- > AdTable
	   * eventInfoTag -- > ImpressionInfoTag
	   * countryInfoTag --> AdInfoTag
	   * eventInfo --> ImpressionInfo
	   * countryInfo -- > AdInfo
	   * CountryCode --> Ad_ID
	   * countryName --> Ad_Info
	   */
	  static PCollection<String> joinEvents(PCollection<TableRow> ImpressionTable,
	      PCollection<TableRow> AdTable) throws Exception {

	    final TupleTag<String> ImpressionInfoTag = new TupleTag<String>();
	    final TupleTag<String> AdInfoTag = new TupleTag<String>();

	    // transform both input collections to tuple collections, where the keys are country
	    // codes in both cases.
	    PCollection<KV<String, String>> ImpressionInfo = ImpressionTable.apply(
	        ParDo.of(new ExtractImpressionDataInfoFn()));
	    PCollection<KV<String, String>> AdInfo = AdTable.apply(
	        ParDo.of(new ExtractAdDataInfoFn()));

	    // country code 'key' -> CGBKR (<event info>, <country name>)
	    PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
	        .of(ImpressionInfoTag, ImpressionInfo)
	        .and(AdInfoTag, AdInfo)
	        .apply(CoGroupByKey.<String>create());

	    // Process the CoGbkResult elements generated by the CoGroupByKey transform.
	    // country code 'key' -> string of <event info>, <country name>
	    PCollection<KV<String, String>> finalResultCollection =
	      kvpCollection.apply(ParDo.named("Process").of(
	        new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
				private static final long serialVersionUID = 1L;

			@Override
	          public void processElement(ProcessContext c) {
	            KV<String, CoGbkResult> e = c.element();
	            String Ad_ID = e.getKey();
	            String Ad_Info = "none";
	            Ad_Info = e.getValue().getOnly(AdInfoTag);
	            for (String eventInfo : c.element().getValue().getAll(ImpressionInfoTag)) {
	              // Generate a string that combines information from both collection values
	              c.output(KV.of(Ad_ID, " " + Ad_Info
	                      + " " + eventInfo));
	            }
	          }
	      }));

	     //write to GCS
	    PCollection<String> formattedResults = finalResultCollection
	        .apply(ParDo.named("Format").of(new DoFn<KV<String, String>, String>() {
	          @Override
	          public void processElement(ProcessContext c) {
	            String outputstring = "AdUnitID: " + c.element().getKey()
	                + ", " + c.element().getValue();
	            c.output(outputstring);
	          }
	        }));
	    return formattedResults;
	  }

	
	/**
	 * Main Method 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	 {
		 // Pipeline Options Configuration
		 DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		 options.setRunner(BlockingDataflowPipelineRunner.class);
		 options.setProject("gcpbigdatapoc");
		 options.setStagingLocation("gs://gcppocbucket/staging/");
		 
		 //Creating a Pipeline 
		 Pipeline p = Pipeline.create(options);
		 
		 //Loading all the csvs to pipeline
		 PCollection<String> Ad_line = p.apply(TextIO.Read.from("gs://gcppocbucket/cloudstorage/20170929/Advertisement.csv").named("Load Advertisement.csv"));
		 PCollection<String> Users_line = p.apply(TextIO.Read.from("gs://gcppocbucket/cloudstorage/20170929/Users.csv").named("Load Users.csv"));
		 PCollection<String> Impression_line = p.apply(TextIO.Read.from("gs://gcppocbucket/cloudstorage/20170929/Impression.csv").named("Load Impression.csv"));
		 
		 //Transforming the PCollection into TableRow
		 PCollection<TableRow> Ad_row = Ad_line.apply("Changing Ad_Line to Ad_row", ParDo.of(new StringToRowConverterAd()).named("Convert Advertisement.csv to Ad Table"));
		 PCollection<TableRow> User_row = Users_line.apply("Changing Users_Line to Users_row", ParDo.of(new StringToRowConverterUser()).named("Changing Users.csv to Users Table"));
		 PCollection<TableRow> Impression_row = Impression_line.apply("Changing Impression_Line to Impression_row", ParDo.of(new StringToRowConverterImpression()).named("Changing Impression.csv to Impression Table"));
			  
		 //Loading the PCollection to BigQuery Table 
		 Ad_row.apply(BigQueryIO.Write.named("Writing to Bigquery").to("gcpbigdatapoc:gcppocdataset.Advertisement").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		 User_row.apply(BigQueryIO.Write.named("Writing to Bigquery").to("gcpbigdatapoc:gcppocdataset.Users").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		 Impression_row.apply(BigQueryIO.Write.named("Writing to Bigquery").to("gcpbigdatapoc:gcppocdataset.Impression").withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		 
		 //Load BigQuery Table into New PCollection
		 PCollection<TableRow> AdTable = p.apply(BigQueryIO.Read.from("gcpbigdatapoc:gcppocdataset.Advertisement"));
		 PCollection<TableRow> UserTable = p.apply(BigQueryIO.Read.from("gcpbigdatapoc:gcppocdataset.Users"));
		 PCollection<TableRow> ImpressionTable = p.apply(BigQueryIO.Read.from("gcpbigdatapoc:gcppocdataset.Impression"));
		 
		 PCollection<String> formattedResults = joinEvents(ImpressionTable, AdTable);
		 formattedResults.apply(TextIO.Write.to("gs://gcppocbucket/cloudstorage/output.txt"));
		 
		 p.run();
		 
	 }

}
