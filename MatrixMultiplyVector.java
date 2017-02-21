package org.ss.mv;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMultiplyVector {

	private static final boolean DEBUG = false;

	private static String inputPathA;
	private static int I;
	private static int IB;
	private static int KB;
	private static int NIB;

	/**
	 * @param conf
	 *            Initializes the global variables from the job context for the mapper and reducer tasks.
	 */
	private static void init(Configuration conf) {
		inputPathA = conf.get("inputPathA");
		I = conf.getInt("I", 1);
		IB = conf.getInt("IB", 1);
		KB = conf.getInt("KB", 1);
		NIB = (I - 1) / IB + 1;
		if (DEBUG) {
			System.out.println("I=" + I + " IB=" + IB + " KB=" + KB + " NIB=" + NIB);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, MyKey, MyValue> {
		private Path path;
		private MyKey myKey = new MyKey();
		private MyValue myValue = new MyValue();
		private boolean matrixA;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			init(context.getConfiguration());
			FileSplit split = (FileSplit) context.getInputSplit();
			path = split.getPath();
			matrixA = path.toString().contains(inputPathA);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer line = new StringTokenizer(value.toString());
			String[] str = new String[3];
			str[0] = line.nextToken();
			str[1] = line.nextToken();
			str[2] = line.nextToken();
			myValue.v = Double.valueOf(str[2]);
			if (matrixA) {
				long i = Long.valueOf(str[0]) - 1;
				long k = Long.valueOf(str[1]) - 1;
				myKey.ib = new VLongWritable(i / IB);
				myKey.kb = new VLongWritable(k / KB);
				byte m = 0;
				myKey.m = new ByteWritable(m);
				myValue.index1 = i % IB;
				myValue.index2 = k % KB;
				context.write(myKey, myValue);
				if (DEBUG) {
					System.out.println("k=" + k + " KB=" + KB + " k/KB=" + k / KB);
					System.out.println(myKey + " : " + myValue);
				}
			} else {
				long k = Long.valueOf(str[0]) - 1;
				myKey.kb = new VLongWritable(k / KB);
				byte m = 1;
				myKey.m = new ByteWritable(m);
				myValue.index1 = k % KB;
				myValue.index2 = 0;
				for (int ib = 0; ib != NIB; ++ib) {
					myKey.ib = new VLongWritable(ib);
					context.write(myKey, myValue);
					if (DEBUG) {
						System.out.println(myKey + " : " + myValue);
					}
				}
			}
		}
	}

	private static class JobPartitioner extends Partitioner<MyKey, MyValue> {
		@Override
		public int getPartition(MyKey key, MyValue value, int numPartitions) {
			return (int) (key.ib.get() % numPartitions);
		}
	}

	public static class Reduce extends Reducer<MyKey, MyValue, NullWritable, Text> {
		private double[][] A;
		private double[][] C;
		private long sib;
		private long skb;

		private Text outValue = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			init(context.getConfiguration());
			A = new double[IB][KB];
			C = new double[IB][1];
			sib = -1;
			skb = -1;
		}

		@Override
		protected void reduce(MyKey key, Iterable<MyValue> valueList, Context context)
				throws IOException, InterruptedException {
			if (DEBUG) {
				System.out.println(key);
			}
			long ib = key.ib.get();
			long kb = key.kb.get();
			if (ib != sib) {
				if (-1 != sib) {
					emit(context, sib);
				}
				sib = ib;
				skb = -1;
				for (int i = 0; i != IB; ++i)
					C[i][0] = 0;
			}
			if (0 == key.m.get()) {
				skb = kb;
				for (int i = 0; i != IB; ++i)
					for (int j = 0; j != KB; ++j)
						A[i][j] = 0;
				for (MyValue myValue : valueList) {
					A[(int) myValue.index1][(int) myValue.index2] = myValue.v;
				}
			} else {
				if (kb != skb)
					return;
				for (MyValue myValue : valueList) {
					for (int i = 0; i != IB; ++i)
						C[i][0] += A[i][(int) myValue.index1] * myValue.v;
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (-1 != sib) {
				emit(context, sib);
			}
		}

		private void emit(Context context, long sib) throws IOException, InterruptedException {
			long ibase = sib * IB;
			for (int i = 0; i != IB; i++) {
				double v = C[i][0];
				if (0 != v) {
					String value = "" + (ibase + i + 1) + " 1 " + v;
					outValue.set(value);
					context.write(null, outValue);
				}
			}
		}
	}

	/**
	 * Prints a usage error message and exits.
	 */
	private static void printUsageAndExit() {
		System.err.println("Usage: hadoop jar MatrixMultiplyVector.jar inputPathA inputPathB outputDirPath R I IB KB");
		System.exit(2);
	}

	/**
	 * @param args
	 *            inputPathA：Path to input file or directory of input files for matrix A.
	 * 
	 *            inputPathB：Path to input file or directory of input files for vector B.
	 * 
	 *            outputDirPath：Path to directory of output files for C = A*B.
	 * 
	 *            R：Number of reduce tasks for job
	 * 
	 *            I：Row dimension of matrix A and vector C.
	 * 
	 *            IB：Number of rows per A block and C block.
	 * 
	 *            KB：Number of columns per A block and rows per B block.
	 * @throws Exception
	 */
	private static void validateAndRun(Configuration conf, String[] args) throws Exception {
		if (args.length != 7) {
			printUsageAndExit();
		}
		String inputPathA = args[0];
		String inputPathB = args[1];
		String outputDirPath = args[2];
		int R = 1;
		int I = 1;
		int IB = 1;
		int KB = 1;
		try {
			R = Integer.valueOf(args[3]);
			I = Integer.valueOf(args[4]);
			IB = Integer.valueOf(args[5]);
			KB = Integer.valueOf(args[6]);
		} catch (NumberFormatException e) {
			System.err.println("Syntax error in integer argument");
			printUsageAndExit();
		}
		if (inputPathA == null || inputPathA.length() == 0)
			throw new Exception("inputPathA is null or empty");
		if (inputPathB == null || inputPathB.length() == 0)
			throw new Exception("inputPathB is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");
		if (R < 1)
			throw new Exception("R must be >= 1");
		if (I < 1)
			throw new Exception("I must be >= 1");
		if (IB < 1 || IB > I)
			throw new Exception("IB must be >= 1 and <= I");
		if (KB < 1 || KB > I)
			throw new Exception("KB must be >= 1 and <= I");

		FileSystem fs = FileSystem.get(conf);
		inputPathA = fs.makeQualified(new Path(inputPathA)).toString();
		inputPathB = fs.makeQualified(new Path(inputPathB)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		conf.set("inputPathA", inputPathA);
		conf.setInt("I", I);
		conf.setInt("IB", IB);
		conf.setInt("KB", KB);

		Job job = new Job(conf, "This is Matrix multiply Vector Job!");
		job.setJarByClass(MatrixMultiplyVector.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(JobPartitioner.class);
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(MyValue.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPathA));
		FileInputFormat.addInputPath(job, new Path(inputPathB));
		FileOutputFormat.setOutputPath(job, new Path(outputDirPath));
		job.setNumReduceTasks(R);
		boolean successful = job.waitForCompletion(true);
		System.out.println(job.getJobID() + (successful ? " successful" : " failed"));
		System.exit(successful ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.out.println("RunningJob Job!");
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		validateAndRun(conf, otherArgs);
	}

}
