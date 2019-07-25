package mapreduce.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class secondMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int sourcePageTabIndex = value.find("\t");
        int sourceRankTabIndex = value.find("\t", sourcePageTabIndex+1);

        if(sourcePageTabIndex == -1) return;
        if(sourceRankTabIndex+1 == -1) return;

        String sourcePage = Text.decode(value.getBytes(), 0, sourcePageTabIndex);

        String sourcePageWithRank = Text.decode(value.getBytes(), 0, sourceRankTabIndex+1);

        context.write(new Text(sourcePage), new Text("!"));

        String outlinksOfSource = Text.decode(value.getBytes(), sourceRankTabIndex+1, value.getLength()-(sourceRankTabIndex+1));
        String[] outlinkPages = outlinksOfSource.split(",");
        int numOfLinks = outlinkPages.length;

        for (String outlinkPage : outlinkPages){
            Text output = new Text(sourcePageWithRank + numOfLinks);
            context.write(new Text(outlinkPage), output);
        }

        context.write(new Text(sourcePage), new Text("|" + outlinksOfSource));
    }
}