package lab1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4 {
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split(",");
            if (parts.length < 2) {
                return;
            }
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            String movieId = parts[0];
            String title = parts[1];
            context.write(new Text(movieId), new Text("M::" + title));
        }
    }

    public static class UserAgeMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            String userId = parts[0];
            String age = parts[2];
            context.write(new Text(userId), new Text("A::" + age));
        }
    }

    public static class UserRatingMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            String userId = parts[0];
            String moviesId = parts[1];
            float rating = Float.parseFloat(parts[2]);
            context.write(new Text(userId), new Text("M::" + moviesId + "::" + rating));
        }
    }

    public static class AgeRatingMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\t");
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            String moviesId = parts[0];
            String ageRating = parts[1];
            context.write(new Text(moviesId), new Text(ageRating));
        }
    }

    public static class Job1Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String age = "";
            List<String> movieRatings = new ArrayList<>();
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.startsWith("A::")) {
                    age = val.substring(3);
                }
                else if (val.startsWith("M::")) {
                    movieRatings.add(val.substring(3));
                }
            }
            if (age.isEmpty()) return;

            for (String movieRating : movieRatings) {
                String[] parts = movieRating.split("::");
                if (parts.length < 2) continue;
                String moviesId = parts[0];
                String rating = parts[1];
                // VD: moviesId  20::4.5
                context.write(new Text(moviesId), new Text(age + "::" + rating));
            }
        }
    }

    public static String ageGroupAllocation(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-45";
        else return "50+";
    }

    public static String ageRatingValue(Map<String, float[]> ageRatingMap) {
        String res = "";
        List<String> ageGroupList = Arrays.asList("0-18", "18-35", "35-45", "50+");
        for (String ageGroup : ageGroupList) {
            if (ageRatingMap.containsKey(ageGroup)) {
                float[] ratingCount = ageRatingMap.get(ageGroup);
                float avgRating = ratingCount[0] / ratingCount[1];
                res += ageGroup + ": " + avgRating + "  ";
                continue;
            }
            res += ageGroup + ": NA  ";
        }
        return res;
    }

    public static class Job2Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = "";
            Map<String, float[]> ageRatingMap = new HashMap<>();
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.startsWith("M::")) {
                    title = val.substring(3);
                }
                else {
                    String[] parts = val.split("::");
                    if (parts.length < 2) continue;
                    int age = Integer.parseInt(parts[0].trim());
                    float rating = Float.parseFloat(parts[1].trim());
                    String ageGroup = ageGroupAllocation(age);
                    ageRatingMap.putIfAbsent(ageGroup, new float[]{0f, 0f});
                    ageRatingMap.get(ageGroup)[0] += rating;
                    ageRatingMap.get(ageGroup)[1] += 1;
                }
            }
            if (title.isEmpty()) return;
            
            context.write(new Text(title), new Text(ageRatingValue(ageRatingMap)));
        }
    }

    //args: users ratings_1 ratings_2 movies output

    public static void main(String[] args) throws Exception {
        String intermediateOutput = args[4] + "_temp";
        String finalOutput = args[4];
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "job1bt4");

        job1.setJarByClass(Bai4.class);
        job1.setReducerClass(Job1Reduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UserAgeMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserRatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, UserRatingMapper.class);

        FileOutputFormat.setOutputPath(job1, new Path(intermediateOutput));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "job2bt4");

        job2.setJarByClass(Bai4.class);
        job2.setReducerClass(Job2Reduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(intermediateOutput), TextInputFormat.class, AgeRatingMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
