package org.example.third;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {


    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //reading value
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        CSVRecord record = parser.iterator().next();


        //getting hashtag
        String hashtag = record.get(2);

        //check hashtag
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

        //getting time
        int time = Integer.parseInt(record.get(0).split(" ")[1].split(":")[0]);

        //check time
        if (!(time >= 9 && time <= 18)) {
            return;
        }

        //getting state field
        String stateField = record.get(18);

        //check state field
        if (stateField == null || stateField.isEmpty())
            return;



        //getting longitude and latitude from csv files
        String latS = record.get(13);
        String longS = record.get(14);

        //check longitude and latitude for empty and null values
        if (latS == null || latS.isEmpty() || longS == null || longS.isEmpty())
            return;

        //convert longitude and latitude to number
        double latitude = Double.parseDouble(latS);
        double longitude = Double.parseDouble(longS);


        //check longitude and latitude for California
        if (longitude < -114.1315 && longitude > -124.6509 && latitude < 42.0126 && latitude > 32.5121) {

            context.write(new Text( candidate+"California"), new IntWritable(1));

            context.write(new Text("California"), new IntWritable(1));
        }

        //check longitude and latitude for newYork
        if (longitude < -71.7517 && longitude > -79.7624 && latitude < 45.0153 && latitude > 40.4772) {

            context.write(new Text(candidate + "York"), new IntWritable(1));

            context.write(new Text("York"), new IntWritable(1));
        }
    }
}