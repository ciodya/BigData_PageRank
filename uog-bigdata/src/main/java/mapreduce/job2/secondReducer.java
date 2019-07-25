package mapreduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class secondReducer extends Reducer<Text, Text, Text, Text> {

    private static final float dampingFactor = 0.85f;

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isPageExisting = false;
        String[] splitRecord;
        float sumPageRanks = 0;
        float newRank = 0;
        String outlinks = "";
        String record;
        int firstSpaceIndex =0;
        int SecondSpaceIndex = 0;

        Text key = new Text(page.toString());

        for (Text value : values){
            record = value.toString();

            if(record.equals("!")) {
                isPageExisting = true;

            }

            else if(record.startsWith("|")){
                outlinks = "\t"+record.substring(1);
            }

            else{
                splitRecord = record.split("\t");

                float pageRank = Float.valueOf(splitRecord[1]);
                int countOutLinks = Integer.valueOf(splitRecord[2]);

                sumPageRanks += (pageRank/countOutLinks);

            }
        }

        if(!isPageExisting) return;
        newRank =  dampingFactor * sumPageRanks + (1 - dampingFactor);

        context.write(key, new Text(newRank + outlinks));
    }
}