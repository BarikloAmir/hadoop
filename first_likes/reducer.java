package org.example.first;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TweetReducer extends Reducer<Text, FloatWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        float sumOfValues = 0;
        for (FloatWritable value : values) {
            sumOfValues += value.get();
        }
        context.write(key, new IntWritable(Math.round(sumOfValues)));
    }
}
