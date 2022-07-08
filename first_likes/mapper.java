package org.example.first;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TweetMapper extends Mapper<Object, Text, Text, FloatWritable> {


    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
            throws IOException, InterruptedException {

        //reading file
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        CSVRecord record = parser.iterator().next();


        //check hashtag
        String hashtag= record.get(2);
        if (hashtag.equals("tweet"))
            return;
        boolean isTrump = hashtag.contains("#Trump") || hashtag.contains("#DonaldTrump");
        boolean isBiden = hashtag.contains("#Biden") || hashtag.contains("#JoeBiden");

        //check what is source
        String sourceOfTweet = record.get(5);

        String source = "";
        if (sourceOfTweet.contains("Web"))
            source = "WebSource";
        else if (sourceOfTweet.contains("iPhone")){
            source = "IPhoneSource";
        }else if(sourceOfTweet.contains("Android")) {
            source = "AndroidSource";
        }


        //check who is candidate
        String candidate;
        if (isBiden && isTrump)
            candidate = "Common";
        else if (isBiden)
            candidate = "Biden";
        else if (isTrump)
            candidate = "Trump";
        else return;


        //get number of retweet
        float numberOfRetweets = Float.parseFloat(record.get(4));
        FloatWritable retweets = new FloatWritable(numberOfRetweets);

        //get number of like
        float numberOfLikes = Float.parseFloat(record.get(3));
        FloatWritable likes = new FloatWritable(numberOfLikes);


        //creating key for like and retweet
        Text likesRecord = new Text(candidate + "Like");
        Text retweetsRecord = new Text(candidate + "Retweet");

        //writing on context
        context.write(likesRecord, likes);
        context.write(retweetsRecord, retweets);

        if (!"".equals(source)){
            Text sourceRecord = new Text(candidate + source);
            context.write(sourceRecord, new FloatWritable(1F));
        }
    }
}