package mapreduce.job3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class thirdMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pageRank = formatTransfer(key, value);

        float score = Float.parseFloat(pageRank[1]);

        Text article_title = new Text(pageRank[0]);
        FloatWritable rank = new FloatWritable(score);

        context.write(article_title,rank);
    }

    private String[] formatTransfer(LongWritable key, Text value) throws CharacterCodingException {
        String[] pageRankPair = new String[2];
        int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex + 1);

        // no tab after rank (when there are no links)
        int lenth;
        if (rankTabIndex == -1) {
            lenth = value.getLength() - (pageTabIndex + 1);
        } else {
            lenth = rankTabIndex - (pageTabIndex + 1);
        }

        pageRankPair[0] = Text.decode(value.getBytes(), 0, pageTabIndex);
        pageRankPair[1] = Text.decode(value.getBytes(), pageTabIndex + 1, lenth);

        return pageRankPair;
    }

}