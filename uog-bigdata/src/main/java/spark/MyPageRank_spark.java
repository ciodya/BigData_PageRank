package spark;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import utils.ISO8601;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MyPageRank_spark {

    public static void main(String[] args) throws ParseException
    {
        //waring: illegal command format
        if (args.length != 4)
        {
            System.out.println("Warning (command format): <input> <output> <iterations> <date>");
            System.exit(1);
        }

        Integer num_of_loops = Integer.parseInt(args[2]);
        long time = ISO8601.toTimeMS(args[3]);

        SparkConf conf = new SparkConf().setAppName("MyPageRank_spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter","\n\n");//split input file into records
        JavaRDD<String> loadData = sc.textFile(args[0]);

        //extract article_title and timestamp from REVISION, outlink list from MAIN
        JavaPairRDD<String, Tuple2<Long, ? extends List<? extends Object>>> data = loadData.mapToPair( (String record) -> {
            String title = "";
            Long t = -1L;
            List<String> outlinks = null;
            for (String s: record.split("\n") )
            {
                if (s.startsWith("REVISION")) {
                    String[] split = s.split(" ");
                    try {
                        t = ISO8601.toTimeMS(split[4]);
                    } catch (ParseException e) {
                        t = -1L;
                    }
                    title = s.split(" ")[3];
                }
                else if (s.startsWith("MAIN")) {
                    outlinks = new ArrayList<>(Arrays.asList(s.split(" "))).stream().distinct().collect(Collectors.toList());
                    outlinks.remove(0);
                    outlinks.add(title);
                }
            }

            //generate tuples containing article title, timestamp, outlinks
            //date of record should be earlier than date specified by user
            if (!title.equals("") && outlinks != null && t <= time)
                return new Tuple2<>(title,new Tuple2<>(t,outlinks));

            return new Tuple2<>("",new Tuple2<>(-1L,new ArrayList<>()));

        }).reduceByKey( ((a,b) -> a._1 > b._1 ? a : b));//filter the latest version of records

        //generate new RDD containing pages that only exist in outlinks
        JavaPairRDD<String, Tuple2<Long, ? extends List<? extends Object>>> data2 = data
                    .flatMapToPair(v -> {
                        List<Tuple2<String, Tuple2<Long, ? extends List<? extends Object>>>> results = new ArrayList<>();

                        for (Object s : v._2._2) {
                            List<String> outlinks = new ArrayList<>(Arrays.asList(s.toString())).stream().distinct().collect(Collectors.toList());
                            results.add(new Tuple2<>(s.toString(),new Tuple2<>(-1L,outlinks)));
                        }
                        return results;
                    });
        //union two RDDs into a new RDD, which contains all pages without duplicates
        JavaPairRDD<String, Tuple2<Long, ? extends List<? extends Object>>> data_tmp = data2.union(data)
                .reduceByKey( ((a,b) -> a._2.size() >= b._2.size() ? a : b));
        //generate initial ranks: 1.0 for all pages
        JavaPairRDD<String, Double> ranks = data_tmp.mapValues(rs -> 1.0);

        //applying PageRank algorithm
        for (int current = 0; current < num_of_loops; current++) {
            JavaPairRDD<String, Double> contribs = data_tmp.join(ranks)
                    .flatMapToPair(v -> {
                        List<Tuple2<String, Double>> results = new ArrayList<>();

                        //calculate number of self-loops in outlinks
                        double num_of_dup = 0.0;
                        for (Object s : v._2._1._2) {
                            if(v._1.equals(s.toString()))
                                num_of_dup++;
                                }
                        //calculate number of out-links of current page, filtering self-loops
                        double urlCount;
                        if(Iterables.size(v._2._1._2)==num_of_dup)
                            urlCount = 1;
                        else
                            urlCount = Iterables.size(v._2._1._2)-num_of_dup;

                        //calculate contributes of pages
                        for (Object s : v._2._1._2) {
                            if(v._1.equals(s.toString()))
                                results.add(new Tuple2<>(s.toString(), 0.0));
                            else
                                results.add(new Tuple2<>(s.toString(), v._2._2 / urlCount));
                        }
                        return results;
                    });

            //deploy PageRank formula, update page ranks
            ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
        }

        //sort results
        ranks = ranks.mapToPair( x -> x.swap()).sortByKey(false).mapToPair( x-> x.swap());

        //generate output file
        ranks.saveAsTextFile(args[1]);
    }
}