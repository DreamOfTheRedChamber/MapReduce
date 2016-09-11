

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> 
{
	
	/* 		
	//<keyword doc1,doc2,doc3...>

	*/
	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException 
	{
		
		// for a word which appears too little, does not put it in the generated inverted index
		int threashold = 100; 

		// for storing the resulting docs, formatted as 'doc1\tdoc2'
		StringBuilder sb = new StringBuilder();

		// after shuffle phase, generated < keyword, docID > pairs are already sorted by keyword
		// use the lastDoc as a sentinel to remove duplicate
		String lastDoc = null;
		int count = 0;
		
		//values <doc1, doc2, doc2, doc2, doc3,..>
		for( Text value: values )
		{
			// lastDoc not set yet
			if(lastDoc == null) 
			{
				lastDoc = value.toString().trim();
				count++;
			}
			// current book is same as lastDoc
			else if( value.toString().trim().equals(lastDoc) )
			{
				count++;
			}
			// current book is different last book, but key word does not appear enough times inside this doc, reset count and lastDoc for next key word
			else if(count < threashold) 
			{
				count = 1;
				lastDoc = value.toString().trim();
			}
			// current book is different last book, but key word does not appear enough times inside this doc
			else 
			{
				sb.append(lastDoc);
				sb.append("\t");
				
				count = 1;
				lastDoc = value.toString().trim();
			}
		}

		// special processing needed for the last word
		if(count >= threashold) 
		{
			sb.append(lastDoc);
		}
		
		// write < key, <DocId, ...> to output
		if(!sb.toString().trim().equals("")) 
		{
			context.write(key, new Text(sb.toString()));
		}
	}
}