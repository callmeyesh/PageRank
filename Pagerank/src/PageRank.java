import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

	private static NumberFormat nf = new DecimalFormat("00");
	private static Long danglingNodeLong;
	private static Float danglingNodefloat;

	public static void main(String[] args) throws Exception {

		int runs = 0;
		for (; runs < 10; runs++) {
			String inputPath = "s3n://alldatapagerank/part-r-000"
					+ nf.format(runs);
			String MR1outputPath = "s3n://alldatapagerank/dangling-r-000"
					+ nf.format(runs + 1);
			String MR2outputPath = "s3n://alldatapagerank/part-r-000"
					+ nf.format(runs + 1);
			// Map Reduce 1: Get Page Rank of dangling nodes
			PageRank.danglingNodes(inputPath, MR1outputPath);
			// Map Reduce 2: Calculate Page Rank of other nodes
			PageRank.pageRank(inputPath, MR2outputPath, danglingNodefloat);
		}
	}

	// Configuration for Map Reduce job 1
	public static void danglingNodes(String inputPath, String outputPath)
			throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Calculate PageRank of Dangling Node");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(danglingNodeMapper.class);
		job.setReducerClass(danglingNodeReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		danglingNodeLong = job.getCounters()
				.findCounter(danglingNodeReduce.UpdateCounter.SUMOFNODES)
				.getValue();
		// Conf supports long data type, convert it back to float.
		danglingNodefloat = ((float) danglingNodeLong) / 1000000000;

	}

	// Configuration for Map Reduce job 2
	public static void pageRank(String inputPath, String outputPath,
			Float danglingNodePR) throws IOException, InterruptedException,
			ClassNotFoundException {

		Configuration conf = new Configuration();
		conf.setFloat("danglingPR", danglingNodefloat);
		Job job = new Job(conf, "Calculate PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(RankCalculateMapper.class);
		job.setReducerClass(RankCalculateReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);

	}

	/**
	 * KEY is an object based on the position of text in input file.
	 * 
	 * VALUE is a single line in the text file.
	 * */
	public static class danglingNodeMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		private Text ONE = new Text("1");
		private static float SumofPR = 0;
		private FloatWritable PAGERANK = new FloatWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			int pageTabIndex = value.find("\t");
			int rankTabIndex = value.find("\t", pageTabIndex + 1);
			String split[] = new String[] {};

			// String page = Text.decode(value.getBytes(), 0, pageTabIndex);
			String pageWithRank = Text.decode(value.getBytes(), 0,
					rankTabIndex + 1);

			split = pageWithRank.split("\\t");
			float pageRank = Float.valueOf(split[1]);

			String links = Text.decode(value.getBytes(), rankTabIndex + 1,
					value.getLength() - (rankTabIndex + 1));
			StringTokenizer allOtherLinks = new StringTokenizer(links, ",");

			int totalLinks = allOtherLinks.countTokens();

			if (totalLinks == 0) {
				SumofPR += pageRank;
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			PAGERANK.set(SumofPR);
			context.write(ONE, PAGERANK);
			SumofPR = 0;
		}
	}

	/**
	 * KEY is a Text (ONE) sent from Map.
	 * 
	 * VALUE is a Iterator of float writable.
	 * */
	public static class danglingNodeReduce extends
			Reducer<Text, FloatWritable, NullWritable, FloatWritable> {

		public static enum UpdateCounter {
			SUMOFNODES
		}
        // Total number of nodes in the data set 
		private Integer TOTALNODES = new Integer(28394585);

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			double s = 0;
			for (FloatWritable val : values) {
				s += val.get();

			}
			long result = (long) (s * 1000000000);
			// Update the value in the counter.
			context.getCounter(UpdateCounter.SUMOFNODES).setValue(
					result / TOTALNODES);
		}
	}

	/**
	 * KEY is a LongWritable
	 * 
	 * VALUE is a single line in the text file.
	 * */
	public static class RankCalculateMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text Page = new Text();
		private Text exclamation = new Text();
		private Text OtherPages = new Text();
		private Text OriginalLink = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			int pageTabIndex = value.find("\t");
			int rankTabIndex = value.find("\t", pageTabIndex + 1);

			String page = Text.decode(value.getBytes(), 0, pageTabIndex);
			String pageWithRank = Text.decode(value.getBytes(), 0,
					rankTabIndex + 1);

			// Mark page as an Existing page
			Page.set(page);
			exclamation.set("!");
			context.write(Page, exclamation);

			// Skip pages with no links.
			if (rankTabIndex == -1)
				return;

			String links = Text.decode(value.getBytes(), rankTabIndex + 1,
					value.getLength() - (rankTabIndex + 1));
			String[] allOtherPages = links.split(",");
			int totalLinks = allOtherPages.length;

			for (String otherPage : allOtherPages) {
				Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
				OtherPages.set(otherPage);
				context.write(OtherPages, pageRankTotalLinks);
			}

			// Put the original links of the page for the reduce output
			OriginalLink.set("|" + links);
			context.write(Page, OriginalLink);
		}
	}

	/**
	 * KEY is a Text (Page id).
	 * 
	 * VALUE is an iterator and it consists of 1. page rank with page id's of
	 * all the links in the page. 2. page id with an exclamations mark.
	 * 
	 * */
	public static class RankCalculateReduce extends
			Reducer<Text, Text, Text, Text> {

		private static final float damping = 0.85F;
		private Text OriginalLinks = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			boolean isExistingWikiPage = false;
			String[] split;
			float sumOfOtherPageRanks = 0;
			String links = "";
			String pageWithRank;

			// For each otherPage:
			// - check control characters
			// - calculate pageRank share
			// - add the share to sumOfOtherPageRanks
			while (values.iterator().hasNext()) {
				pageWithRank = values.iterator().next().toString();

				if (pageWithRank.equals("!")) {
					isExistingWikiPage = true;
					continue;
				}

				if (pageWithRank.startsWith("|")) {
					links = "\t" + pageWithRank.substring(1);
					continue;
				}

				split = pageWithRank.split("\\t");

				float pageRank = Float.valueOf(split[1]);
				float countOutLinks = Integer.valueOf(split[2]);

				sumOfOtherPageRanks += (pageRank / countOutLinks);
			}

			if (!isExistingWikiPage)
				return;

			// get the dangling node page rank from the conf variable.
			String confVariable = context.getConfiguration().get("danglingPR");
			float danglingNodePR = Float.parseFloat(confVariable);

			float newRank = damping * (sumOfOtherPageRanks + danglingNodePR)
					+ (1 - damping);

			OriginalLinks.set(newRank + links);
			context.write(key, OriginalLinks);
		}
	}

}
