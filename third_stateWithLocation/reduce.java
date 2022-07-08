package org.example.third;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TweetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        float sum_of_values = 0;
        for (IntWritable value : values) {
            sum_of_values += value.get();
        }
        context.write(key, new IntWritable(Math.round(sum_of_values)));
    }
}
