package org.example.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class mapper
        extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        @SuppressWarnings("deprecation")
        Job job = new Job(getConf(), "states tweet counter");

        job.setMapperClass(TweetMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TweetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int size = args.length;
        for (int i = 0; i < size - 1; i++)
            FileInputFormat.addInputPath(job, new Path(args[i]));
        FileOutputFormat.setOutputPath(job, new Path(args[size - 1]));

        boolean result = job.waitForCompletion(true);
        if (!result)
            return 1;

        //this function show output in terminal
        terminal_show(args[size - 1]);

        return 0;
    }

    public static void terminal_show(String path) throws IOException {
        //configuration of hadoop fileSystem
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(new Path(path), true);

        //getting path
        Path result_file = null;
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
            Path p = fileStatus.getPath();
            if (p.toString().contains("part-r-")) {
                result_file = p;
                break;
            }
        }

        //opening output file
        FSDataInputStream stream = fileSystem.open(result_file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));


        Map<String, Integer> keyValueHashMap = new HashMap<>();
        String line = reader.readLine();
        while (line != null) {
            //getting key and value in keyValue list
            String[] keyValue = line.split("\\s+");
            //getting key from keyValue list
            String key = keyValue[0];
            //getting value from keyValue list
            int value = Integer.parseInt(keyValue[1]);

            keyValueHashMap.put(key, value);

            //getting to next line
            line = reader.readLine();
        }

        String[] states = {"York", "Texas", "California", "Florida"};

        System.out.println("*********************************************************************************************");
        System.out.println("************************** Final Output ******************************************************");
        System.out.println("**********************************************************************************************");
        System.out.println("state  |        Trump     |         Biden     |         common     |        all_of_tweet      ");

        for (String state : states) {
            int allOfTweet = 0;
            double percentOfCommon = 0;
            double percentOfTrump = 0;
            double percentOfBiden = 0;

            //getting number of all tweet of this state
            Integer numberOfTweet = keyValueHashMap.get(state);

            if (numberOfTweet != null) {
                allOfTweet = numberOfTweet;
                percentOfCommon = keyValueHashMap.get(state + "Common") / (double) allOfTweet;
                percentOfTrump = keyValueHashMap.get(state + "Trump") / (double) allOfTweet;
                percentOfBiden = keyValueHashMap.get(state + "Biden") / (double) allOfTweet;
            }

            String outState = state.equals("York") ? "New York" : state;


            System.out.println(outState + "   " + percentOfTrump + "   " +
                    percentOfBiden + "   " + percentOfCommon + "    " + allOfTweet);
        }
        System.out.println("**********************************************************************************************");
        System.out.println("**********************************************************************************************");
        System.out.println("**********************************************************************************************");
    }

    public static void main(String[] args) throws Exception {
        int jobStatus = ToolRunner.run(new TweetDriver(), args);
        String status;
        if (jobStatus==1){
            status = "unsuccessfully";
        }
        else {
            status = "successfully";
        }
        System.out.println("-------------job status -------------");
        System.out.println(status);
        System.out.println("-------------------------------------");


    }
}
