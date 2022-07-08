package org.example.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.FloatWritable;
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

public class main extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        @SuppressWarnings("deprecation")
        Job job = new Job(getConf(), "likes , retweets and source counter for trump and biden");

        job.setMapperClass(TweetMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setReducerClass(TweetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int size = args.length;
        for (int i = 0; i < size - 1; i++)
            FileInputFormat.addInputPath(job, new Path(args[i]));
        FileOutputFormat.setOutputPath(job, new Path(args[size - 1]));

        boolean res = job.waitForCompletion(true);
        if (!res)
            return 1;

        terminal_show(args[size - 1]);

        return 0;
    }

    public static void terminal_show(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(path), true);
        Path result_file = null;
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
            Path p = fileStatus.getPath();
            if (p.toString().contains("part-r-")) {
                result_file = p;
                break;
            }
        }
        FSDataInputStream stream = fs.open(result_file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        System.out.println("**********************************************************************");
        System.out.println("************************** Final Output ******************************");
        System.out.println("**********************************************************************");
        System.out.println("candidate  |  like  |  retweet  |   web   |  iphone   |   android    |");
        String newLine = reader.readLine();
        while (newLine != null) {

            //get value of this keys
            String android = "", iPhone = "", web = "" , like = "", retweet = "" ;

            String[][] lines = new String[5][2];
            lines[0] = reader.readLine().split("\\s+");
            lines[1] = reader.readLine().split("\\s+");
            lines[2] = reader.readLine().split("\\s+");
            lines[3] = reader.readLine().split("\\s+");
            lines[4] = newLine.split("\\s+");

            //find what is key
            String key = lines[0][0];
            if (key.contains("Biden"))
                key = "Biden";
            else if (key.contains("Trump"))
                key = "Trump";
            else key = "both";

            //find value of each key
            for (String[] line : lines) {
                if (line[0].contains("Web")) {
                    android = line[1];
                } else if (line[0].contains("IPhone")) {
                    iPhone = line[1];
                } else if (line[0].contains("Android")) {
                    web = line[1];
                } else if (line[0].contains("Like")) {
                    like = line[1];
                } else {
                    retweet = line[1];
                }
            }


            System.out.println( key+"    " +like+"      "
                   +   retweet+"       "
                   +  android+"     "
                   + iPhone+"      "
                   +   web+"    ");
            newLine = reader.readLine();
        }
        System.out.println("**********************************************************************");
        System.out.println("**********************************************************************");
        System.out.println("**********************************************************************");
        System.out.println();
        System.out.println();
    }


    public static void main(String[] args) throws Exception {
        int jobStatus = ToolRunner.run(new TweetDriver(), args);
        String status;
        if (jobStatus==1){
            status = "unsuccessfully";
        }
        else {
            status="successfully";
        }
        System.out.println("-------------job status -------------");
        System.out.println(status);
        System.out.println("-------------------------------------");
    }
}
