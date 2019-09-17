package assignment1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * Note: The input format can be: 2 1 /tmp(absolute path)/input output
 * input folders may have file01.txt, file02.txt, file03.txt, file04.txt and so on.
 * output folders may get some output file, sometimes it may hint that this is an excutable file,
 * using gedit or change the name with .txt can open that file.
 *
 */

public class Assignment1 {
	
	private static int nb_of_ngrams;       //The value N for the ngram. It is args[0].
	private static int min_count_ngrams;   // The minimum count for an ngram to be included in the output file. It is args[1]
  
// TODO: Write the source code for your solution here.	
	public static class TokenizerMapper
		extends Mapper<Object, Text, Text, Text> {
		
			private Text output_value = new Text();    // output value, the format is "filename"
			private Text word = new Text();         // key outuput value (to reduce function)
			private String word1 = new String();   // key intermediate value
			private ArrayList<String> all_words = new ArrayList<String>();    //getting all contents from a file
			private String word_inter = new String();     // getting fragment words from a file
			private String word_trans;
			public int n = nb_of_ngrams;      // the n for ngrams, if it is bingrams, n = 2
			
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			// First, getting the filename
			InputSplit inputSplit =context.getInputSplit(); // get belong file
	        String fileName = ((FileSplit)inputSplit).getPath().getName();

			
			//Second, get all the contents of files and store in "all_words"
			while (itr.hasMoreTokens()) {
				word_inter = itr.nextToken();
				all_words.add(word_inter);
			}
			

			//Third, get the ngrams.
			//In the begining, the word1 means the first word in ngrams. From word1, to get the remaining n-1 words in ngrams(word1 is the first word.)
			//Then, word1 as the first some words, word_trans is the j th word and add it to word1.
			//Finally, if there are already n words in word1(which means word1 has n words and satisfies the ngrams.), break the loop.
			for(int i= 0; i<all_words.size();i++) {
				word1 = all_words.get(i);   //The first word in ngrams
				int j = i+1;
				while(n>1 && j<all_words.size()) {  //Getting the remaining n-1 words in ngrams
					word_trans = " " + all_words.get(j).toString();     
					word1 = word1 + word_trans;     //Add word_trans to word1
					 j++;
					 n--;
				}
				if (n==1) {           //word1 is already ngrams now, return the key and the value which means send the key and the value to reduce function
				word.set(word1);
				output_value.set(fileName);
				//put the key and the value to reducer
				context.write(word, output_value);
				}
				n=nb_of_ngrams;      //reset the value of n
			}
			}
		
	}
	
	public static class IntSumReducer
		extends Reducer<Text,Text,Text,Text> {

	private Text result = new Text();
	private String count = new String();
	private String getFileName = new String();
	private List<String> FileName_list = new ArrayList<String>();
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		////data from map will be <”word”,{filename,filename,..}>, so we get it with an Iterator and thus we can go through the sets of values
		
	int sum = 0;
	for (Text val : values) {      //val is the input value from map, like {file01.txt}, {file02.txt} and {file03.txt}
	sum ++;
	getFileName = val.toString();
	if (!(FileName_list.contains(getFileName))) {    //If the filename doesn't exist before, add the filename to FileName_list.
	FileName_list.add(getFileName);                  //Since some words may appear in different files, we need to show them and I build the FileName_list 
	}                                                //The value may be {file01.txt file03.txt}
	}
	
	if (sum >= min_count_ngrams){
		count = String.valueOf(sum);
		//Collections.sort(FileName_list);   sorting the filename
		result.set(count + "  " + String.join(" ", FileName_list));    //Return the result
		context.write(key, result);
	}

	FileName_list.clear();
	}
	}


public static void main(String[] args) throws Exception { 
	
	//Getting input from command line and then catch input numbers errors
	try {
	nb_of_ngrams = Integer.parseInt(args[0]);
	min_count_ngrams = Integer.parseInt(args[1]);
	if (nb_of_ngrams < 0 || min_count_ngrams <0) {
		throw new Exception();
	}
	}
	
	catch (Exception ex) {
		ex.printStackTrace();
		System.out.print("Wrong input type, Please input two positive integers in first two parameters!");
	}
	
	
	// catch input and output file errors
	try {
	File input_file_path = new File(args[2]);
	File output_file_path = new File(args[3]);
	if(!input_file_path.exists()) {
		throw new FileNotFoundException();
	}
	if(output_file_path.exists()) {
		throw new IOException();
	}
	}
	
	// Input file does't exist
	catch (FileNotFoundException file) {
		file.printStackTrace();
		System.out.print("Input file path doesn't exist! Please input absolute path name!");
	}
	
	// Output file already exists
	catch (IOException file2) {
		file2.printStackTrace();
		System.out.print("Output file already exists! Please delete first!");
	}
	
	//Creating a Configuration object and a Job object, assigning a job name for identification purposes
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "word count");
	
    //Setting the job's jar file by finding the provided class location	
	job.setJarByClass(Assignment1.class); 
	
	//Providing the mapper and reducer class names
	job.setMapperClass(TokenizerMapper.class); 
	job.setReducerClass(IntSumReducer.class); 
	
	//Setting configuration object with the Data Type of output Key and Value for map and reduce
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	// The HDFS input and output directory to be fetched from the command line
	FileInputFormat.addInputPath(job, new Path(args[2])); 
	FileOutputFormat.setOutputPath(job, new Path(args[3])); 
	
	// Submit the job to the cluster and wait for it to finish
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
