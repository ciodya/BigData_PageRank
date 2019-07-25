package mapreduce.job1;

import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import java.util.stream.Collectors;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
    private String userISO_8601;

    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        userISO_8601 = conf.get("ISO_8601");
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String titles;
        String outlink;
        String timestampOfRevision;
        String date;
        List<String> outlinkList = new ArrayList<String>();

        Date userTime = new Date();
        Date recordTime = new Date();

        int start = value.find("REVISION");
        for (int i = 0; i < 3; i++) {
            start = value.find(" ", start + 1);
        }
        int end = value.find(" ", start + 1);
        start += 1;

        titles = Text.decode(value.getBytes(), start, end - start);
        Text target_article_title = new Text(titles);

        start = value.find(" ", start + 1);
        end = value.find(" ", start + 1);

        timestampOfRevision = Text.decode(value.getBytes(), start, end - start);

        try {
            userTime = new Date(utils.ISO8601.toTimeMS(userISO_8601));
            recordTime = new Date(utils.ISO8601.toTimeMS(timestampOfRevision));
            if (userTime.before(recordTime))
                return;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        start = value.find("MAIN");
        int mark = value.find("TALK");
        int run = 0;

        while (end + 1 < mark) {
            start = value.find(" ", start + 1);
            end = value.find(" ", start + 1);
            if (end > mark)
                end = value.find("\n", start + 1);

            start += 1;

            outlink = Text.decode(value.getBytes(), start, end - start);

            //check self-loop
            if (outlink == target_article_title.toString())
                continue;

            //check same outlink
            outlinkList.add(outlink);
        }

        outlinkList.stream().distinct().collect(Collectors.toList());
        for(int i = 0;i < outlinkList.size(); i++ )
            context.write(target_article_title, new Text(timestampOfRevision + "\t" + outlinkList.get(i)));

    }
}
