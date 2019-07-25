package mapreduce.job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Collections;
import java.util.*;
import java.lang.*;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class reducer extends Reducer<Text, Text, Text, Text> {
    final Log log = LogFactory.getLog(reducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isFirst = true;
        String initial_score = "1.0\t";
        int i = 0;
        Date latestDate = new Date();

        for (Text value : values) {
            log.info("value is : " + value);
            int spaceIndex = value.find("\t");
            String temp = Text.decode(value.getBytes(), 0, spaceIndex);
            try {
                Date date = new Date(utils.ISO8601.toTimeMS(temp));
                if(isFirst){
                    latestDate = date;
                }
                else
                    if(latestDate.before(date))
                        latestDate = date;
                context.write(key, new Text(temp));

            } catch (ParseException e) {
                e.printStackTrace();
            }
            isFirst = false;


        }

        for (Text value : values) {
            int spaceIndex = value.find("\t");
            String temp = Text.decode(value.getBytes(), 0, spaceIndex);
            String outlink = Text.decode(value.getBytes(), spaceIndex + 1, value.getLength() - (spaceIndex + 1));
            try {
                Date date = new Date(utils.ISO8601.toTimeMS(temp));
                if(date.equals(latestDate)){
                    if(i > 0)
                        initial_score += "," + outlink;
                    else if(i == 0)
                        initial_score += outlink;
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            i++;
        }

//        context.write(key, new Text(initial_score));
    }
}

