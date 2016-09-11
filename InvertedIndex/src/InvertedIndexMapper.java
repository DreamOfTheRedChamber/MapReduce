

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	private Set<String> stopWords = new ArrayList<String>();
	
	// build a list of stop words, reuse stopwords across different mappers
	@Override
	protected void setup(Context context) throws IOException 
	{
		
		Configuration conf = context.getConfiguration();
		String filePath = conf.get("filePath");
		
		Path pt=new Path(filePath);//Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        while (line != null)
        {
            stopWords.add(line.trim().toLowerCase());
            line=br.readLine();
        }
	}
	
	/* generate <Word, DocId> pairs for building inverted index
	*/
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		// get DocID
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Text name = new Text(fileName);
		
		// get all non-stop words and 
		StringTokenizer tokenizor = new StringTokenizer(value.toString());
		while(tokenizor.hasMoreTokens()) 
		{
			// clean up/generalize curWord
			String curWord = tokenizor.nextToken().toString().toLowerCase();
			curWord = curWord.replaceAll("[^a-zA-Z]", "");

			// filter out the stop word
			if(!stopWords.contains(curWord))
			{
				context.write(new Text(curWord), name);
			}
		}
	}
}
