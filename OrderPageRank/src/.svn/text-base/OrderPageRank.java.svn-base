import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OrderPageRank {

	public static void main(String[] args) throws Exception {
		String inputPath = "s3n://alldatapagerank/part-r-000105";
		String MR1outputPath = "s3n://orderedpg/output2";
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Order By Page Rank");
		job.setJarByClass(OrderPageRank.class);
		job.setMapperClass(RankingMapper.class);
		// job.setCombinerClass(IntCombiner.class);
		// job.setReducerClass(WikiReducer.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(MR1outputPath));
		job.waitForCompletion(true);
	}

	public static class RankingMapper extends
			Mapper<Object, Text, FloatWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] pageAndRank = getPageAndRank(key, value);

			float parseFloat = Float.parseFloat(pageAndRank[1]);

			Text page = new Text(pageAndRank[0]);
			FloatWritable rank = new FloatWritable(parseFloat);

			context.write(rank, page);
		}

		private String[] getPageAndRank(Object key, Text value)
				throws CharacterCodingException {

			String[] pageAndRank = new String[2];
			int tabPageIndex = value.find("\t");
			int tabRankIndex = value.find("\t", tabPageIndex + 1);

			// no tab after rank (when there are no links)
			int end;
			if (tabRankIndex == -1) {
				end = value.getLength() - (tabPageIndex + 1);
			} else {
				end = tabRankIndex - (tabPageIndex + 1);
			}

			pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
			pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1,
					end);

			return pageAndRank;
		}

	}
}
