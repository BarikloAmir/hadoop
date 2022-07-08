package org.example.second;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static String[] states = {"York", "Texas", "California", "Florida"};



    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //reading from value
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        CSVRecord record = parser.iterator().next();
        //get hashtag
        String hashtag = record.get(2);
        if (hashtag.equals("tweet"))
            return;
        //who is candidate
        boolean isTrump = hashtag.contains("#Trump") || hashtag.contains("#DonaldTrump");
        boolean isBiden = hashtag.contains("#Biden") || hashtag.contains("#JoeBiden");

        String candidate;
        if (isBiden && isTrump)
            candidate = "Common";
        else if (isTrump)
            candidate = "Trump";
        else if (isBiden)
            candidate = "Biden";
        else return;

        //check state field
        String stateField = record.get(18);
        if (stateField == null || stateField.isEmpty())
            return;
        stateField = stateField.toLowerCase();

        //check time
        int time = Integer.parseInt(record.get(0).split(" ")[1].split(":")[0]);
        if (!(time >= 9 && time <= 18)) {
            return;
        }

        //writing key value to context
        for (String state : states) {
            if (stateField.contains(state.toLowerCase())) {
                context.write(new Text(state), new IntWritable(1));
                context.write(new Text(state + candidate), new IntWritable(1));
            }
        }
    }
}