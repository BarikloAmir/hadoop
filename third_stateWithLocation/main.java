package org.example.third;

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

public class main extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        @SuppressWarnings("deprecation")
        Job job = new Job(getConf(), "state tweet (from locations )");

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

        show_terminal(args[size - 1]);

        return 0;
    }

    public static void show_terminal(String path) throws IOException {
        //set hadoop file system configuration
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(new Path(path), true);
        Path result_file = null;
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
            Path p = fileStatus.getPath();
            if (p.toString().contains("part-r-")) {
                result_file = p;
                break;
            }
        }
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

        String[] states = {"York", "California"};

        System.out.println("**********************************************************************************************");
        System.out.println("************************** Final Output ******************************************************");
        System.out.println("**********************************************************************************************");
        System.out.println("state    |        Trump     |         Biden     |         common     |        all_of_tweet      ");


        for (String state : states) {
            int allOfTweet = 0;
            double percentOfCommon = 0;
            double percentOfTrump = 0;
            double percentOfBiden = 0;


            //getting number of all tweet of this state
            Integer numberOfTweet = keyValueHashMap.get(state);

            if (numberOfTweet != null) {
                allOfTweet = numberOfTweet;
                percentOfCommon = keyValueHashMap.get( "Common"+state) / (double) allOfTweet;
                percentOfTrump = keyValueHashMap.get("Trump"+state) / (double) allOfTweet;
                percentOfBiden = keyValueHashMap.get( "Biden"+state) / (double) allOfTweet;
            }

            String outState = state.equals("York") ? "New York" : state;

            System.out.println(outState + "   " + percentOfTrump + "   " +
                    percentOfBiden + "   " + percentOfCommon + "    " + allOfTweet);
        }
        System.out.println("**********************************************************************************************");
        System.out.println("**********************************************************************************************");
        System.out.println("**********************************************************************************************");    }

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
