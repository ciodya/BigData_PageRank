# Big_Data_Coursework2
README
===============================================
There are six parts of RDD processes in this project.

### First RDD (data)
-----------------------------------------------
In the beginning, the data preprocessing is applied for reading and collecting the data are needed.
Here is the sample RDD of dummy data:

 Key      |          Values       |
----------| ----------------------|
  A       | <timestamp, [B,C,A,A]>| 
----------| ----------------------|
  B       | <timestamp, [D,A,B]>  |
----------| ----------------------|
  C       | <timestamp, [D,B,C]>  |



### Second RDD (data2)
-----------------------------------------------
In the second step, the outlinks are extracted as key and added tuple2<-1, [self]> as value.
The aim of this step is to add pages that only exist in outlink lists, since the given article titles set and outlink set are not equal. If these pages are not
added to the original article titles set, some results would be lost. Here is the sample RDD of dummy data:

 Key      |          Values       |
----------| ----------------------|
  A       |       <-1, [A]>       |
----------| ----------------------|
  B       |       <-1, [B]>       |
----------| ----------------------|
  C       |       <-1, [C]>       |
----------| ----------------------|
  D       |       <-1, [D]>       |


### Third RDD (data_tmp)
-----------------------------------------------
In the third step, the union RDD of the first and second RDD is constructed as the third RDD.Its keys are the article titles, which include all the unique pages, and
its values are the its timestamp/-1L and outlink list.
The aim of this step is to include all the pages in an RDD. Here is the sample RDD of dummy data:

 Key      |          Values       |
----------| ----------------------|
  A       | <timestamp, [B,C,A,A]>|
----------| ----------------------|
  B       | <timestamp, [D,A,B]>  |
----------| ----------------------|
  C       | <timestamp, [D,B,C]>  |
----------| ----------------------|
  D       |       <-1, [D]>       |



### Fourth RDD (ranks)
-----------------------------------------------
In the fourth step, an initial rank sore 'ranks' is generated and set as a mapValues for records. After each loop of PageRank algorithm, the values of this RDD will be updated. Here is the sample RDD of dummy data:

Initial value:

 Key      |          Values       |
----------| ----------------------|
  A       |            1.0        |
----------| ----------------------|
  B       |            1.0        |
----------| ----------------------|
  C       |            1.0        |
----------| ----------------------|
  D       |            1.0        |


Updated value:

 Key      |          Values       |
----------| ----------------------|
  A       |            0.5        |
----------| ----------------------|
  B       |            0.5        |
----------| ----------------------|
  C       |            0.5        |
----------| ----------------------|
  D       |            1.0        |


### Fifth RDD (contribs)
------------------------------------------------
In the fifth step, the aim of this step is to calculate contributions of each page to other pages. Here is the sample RDD of dummy data:

 Key      |          Values       |
----------| ----------------------|
  A       |            0.5        |
----------| ----------------------|
  B       |            0.5        |
----------| ----------------------|
  C       |            0.5        |
----------| ----------------------|
  D       |            0.0        |


### Sixth RDD (ranks)
------------------------------------------------
In the sixth step, the new PageRank score of each title is applied and order the higher score page to show earlier in the list. Here is the sample RDD of dummy data:

 Key      |          Values       |
----------| ----------------------|
  B       |            1.0        |
----------| ----------------------|
  D       |            1.0        |
----------| ----------------------|
  A       |           0.575       |
----------| ----------------------|
  C       |           0.575       |


OPTIMIZATION
================================================ 

1. Article titles set equals to outlink list set.
2. Filter self-loops.
3. Use java Lambda to simplify the code.



CODE WITH COMMENTS
================================================ 

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
	//Split original data by "\n\n" to collect each REVISION data
        sc.hadoopConfiguration().set("textinputformat.record.delimiter","\n\n");
        JavaRDD<String> loadData = sc.textFile(args[0]);

        //extract article_title and timestamp from REVISION, outlink list from MAIN
        JavaPairRDD<String, Tuple2<Long, ? extends List<? extends Object>>> data = loadData.mapToPair( (String record) -> { 

	    //Map (article_title, (t, outlink)) into T（K,V）
	    //Collect latest (article_title, (t, outlink)) 
            //Define initial variables "revision" as "", 't' as "-1L", external as null
	    String title = "";
            Long t = -1L;
            List<String> outlinks = null;

	    //In each revision, each row is split by '\n'	
            for (String s: record.split("\n") )
            {
		//To find the HEAD with 'REVISION' and split elements in the list
                if (s.startsWith("REVISION")) {
                    String[] split = s.split(" ");
                    try {

			//Collect the fourth value in the list and transfer the datatype into 'MS' then set it as "t" value
                        t = ISO8601.toTimeMS(split[4]);
                    } catch (ParseException e) {
                        t = -1L;
                    }

		    //Collect the third value as "revision" value
                    title = s.split(" ")[3];
                }

		//To find the HEAD with 'MAIN'and split elements in the list
                else if (s.startsWith("MAIN")) {

		    //Generate an arraylist before collecting all values into the array and then do the dintinct for removing duplicate
                    outlinks = new ArrayList<>(Arrays.asList(s.split(" "))).stream().distinct().collect(Collectors.toList());

		    //Remove the first element 'MAIN'
                    outlinks.remove(0);
                    outlinks.add(title);
                }
            }

            //generate tuples containing article title, timestamp, outlinks
            //date of record should be earlier than date specified by user
            if (!title.equals("") && outlinks != null && t <= time)
                return new Tuple2<>(title,new Tuple2<>(t,outlinks));

            return new Tuple2<>("",new Tuple2<>(-1L,new ArrayList<>()));

        }).reduceByKey( ((a,b) -> a._1 > b._1 ? a : b));  //filter the latest version of records

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
	//Specified numbers of loops
        for (int current = 0; current < num_of_loops; current++) {

	    //Join the ranks to each data_tmp as T(K,V)=T((title, (t, outlink)), rankscore)
            JavaPairRDD<String, Double> contribs = data_tmp.join(ranks)

		     //Flatten each 'contribs' list which is outlink lists, then define a new variable 'v' as a tuple((t, outlink), rankscore) 
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
				//For each outlink, calaulate the (PR(v)/L(v)) value with (rankscore/urlCount) then add it to the 'result' as T((t, outlink), (PR(v)/L(v)))
                                results.add(new Tuple2<>(s.toString(), v._2._2 / urlCount));
                        }
                        return results;
                    });

            //deploy PageRank formula, update page ranks
	    //The new 'ranks' can be recalcurated by 0.15 + 0.85 * Sum(PR(v)/L(v))
            ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
        }

        //sort results
	//Update the new 'ranks' value generated from the second RDD process
	//Swap the T(Key, Value) to T(Value, Key), T((t,outlink), pagerankscore) to T(pagerankscore, (t,outlink)) then sort by key (pagerankscore)
	//Swap it back to T((t,outlink), pagerankscore)
        ranks = ranks.mapToPair( x -> x.swap()).sortByKey(false).mapToPair( x-> x.swap());

        //generate output file
        ranks.saveAsTextFile(args[1]);
    }
}


 



