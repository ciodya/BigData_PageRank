# Big_Data_Coursework1
README
==============================================
* We have separated the task into three processes of job, each of them includes mapper step and previous two with reducer parser. 
## Job1
-----------------------------------------------
* In Job1, the latest records of article is filtered based on time_stamp
  * MyRecordReader.java: Splite the data file(.txt) into records are needed in later process.
  * mapper.java: 
    > Filter the key and value we need. 
  
      (Key) article_title: page title 
      
        #set start=‘REVISION’, find the third space, move forward for one element.
        
      (Value) time_stamp: the exact date and time of the revision
      
        #set start=‘REVISION’, find the fifth space, move forward for one element.
        
      (Value) MAIN: title of the outside links
      
        #set start='MAIN', end='TALK', collect data between the start and the end.
      Key  | Values|
      --------- | --------|
      article_title  | time_stamp MAIN |
      ex.  Kelsey_Grammer  | 2007-02-18T03:32:53Z Matt_Lauer San_Diego Toy_Story_2 George_W._Bush... |
    
  * reducer.java:
    > Tranfer the time_stamp into ISO and then date format, and do the comparison for keeping the latest records of the articles then combine the values with key.
    * Apply the function in ISO 8601.java for transfering the time_stamp into ISO format.
    * Transter time_stamp from ISO to date format.
    * Keep latest records with the largest time_stamp and do the grouping by ','.
    * Set the initial value:1 for PageRank score.
    * Set '\t' for 
    > 
      Key  | Values|
      --------- | --------|
      article_title1  | time_stamp1 MAIN[i] |
      article_title1  | time_stamp2 MAIN[j] |
      article_title1  | time_stamp3 MAIN[k] |
      article_title1  | time_stamp2 MAIN[l] |
    
    * if time_stamp2 is the latest record, the output of job1 would be:
    >
     Key  | Values|
     --------- | --------|
     article_title | PageRankScore time_stamp manin|
     article_title1  | 1 MAIN[j] MAIN[l] |
     ex. article_title1  | 1 Matt_Lauer,San_Diego,Toy_Story_2,George_W._Bush... |
 * Generate the file iter00 as the result of job1.

## Job2
----------------------------------------------- 
* In Job2, the PR formula shown below is applied for PR score cauculation, 
  PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)), ?v: ?(v,u) ?S, where L(v) is the number of out-links of page v.
  * secondMapper.class
     >
         * Input '!' as the new value

       Key  | Values|
       --------- | --------|
       article_title  | ! |   

         * Input '|' as the new value
     >  
       Key  | Values|
       --------- | --------|
       article_title  | '|' MAINS | 

         * Switch MAINS as key, article_title, PageRankScore as values
     > 
       Key  | Values|
       --------- | --------|
       MAIN | article_title, 1, PR(v)/L(v))
  
  * secondReducer.class
    There are three types of condition:
    - If '!' in value, nothing to do with.
    - ElseIf '|' in value, L(v)='\t'MAINS.
    - Else do the computation shown below.
    
    Title | RankScore | NumbersOfOutlink
    --------- | --------| --------|
    article_title1 | 1.0 | n1 |
    article_title2 | 1.0 | n2 |
    article_title3 | 1.0 | n3 |

    > NewRankScore=0.15 + 0.85 * Sum(RankScore/NumbersOfOutlink)

   * The output of job2 would be:

     Key | Values
     --------- | --------|
     article_title1 | NewRankScore '\t'MAINS |

* User can set the parameters of running times, in general, more running times can show more accuracy results of PR scores.
* Generate the file iter** as the result of job2.

## Job3
-----------------------------------------------
 * In Job3, the final result is generated as below.  
  * thirdMapper.class
  >
   Key | Values
   --------- | --------|
   article_title1 | NewRankScore |
  * Generate the final file as the result of job3 in the structure above.
