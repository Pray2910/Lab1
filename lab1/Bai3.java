package lab1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {
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

    public static class UserGenderMapper extends Mapper<Object, Text, Text, Text> {
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
            String gender = parts[1];
            context.write(new Text(userId), new Text("G::" + gender));
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

    public static class GenderRatingMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\t");
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            String moviesId = parts[0];
            String genderRating = parts[1];
            context.write(new Text(moviesId), new Text(genderRating));
        }
    }

    public static class Job1Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String gender = "";
            List<String> movieRatings = new ArrayList<>();
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.startsWith("G::")) {
                    gender = val.substring(3);
                }
                else if (val.startsWith("M::")) {
                    movieRatings.add(val.substring(3));
                }
            }
            if (gender.isEmpty()) return;

            for (String movieRating : movieRatings) {
                String[] parts = movieRating.split("::");
                if (parts.length < 2) continue;
                String moviesId = parts[0];
                String rating = parts[1];
                // VD: moviesId  Male::4.5
                context.write(new Text(moviesId), new Text("R::" + gender + "::" + rating));
            }
        }
    }

    public static class Job2Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = "";
            int maleCounting = 0;
            int femaleCounting = 0;
            float maleRating = 0;
            float femaleRating = 0;
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.startsWith("M::")) {
                    title = val.substring(3);
                }
                else if (val.startsWith("R::")) {
                    String[] parts = val.split("::");
                    if (parts.length < 2) continue;
                    if ("M".equals(parts[1])) {
                        maleCounting++;
                        maleRating += Float.parseFloat(parts[2]);
                    }
                    else if ("F".equals(parts[1])) {
                        femaleCounting++;
                        femaleRating += Float.parseFloat(parts[2]);
                    }
                }
            }
            if (title.isEmpty()) return;
            float maleAvg = maleRating / maleCounting;
            float femaleAvg = femaleRating / femaleCounting;
            String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);

            context.write(new Text(title), new Text(result));
        }
    }

    //args: users ratings_1 ratings_2 movies output
    public static void main(String[] args) throws Exception {
        String intermediateOutput = args[4] + "_temp";
        String finalOutput = args[4];
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "job1bt3");

        job1.setJarByClass(Bai3.class);
        job1.setReducerClass(Job1Reduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UserGenderMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserRatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, UserRatingMapper.class);

        FileOutputFormat.setOutputPath(job1, new Path(intermediateOutput));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "job2bt3");

        job2.setJarByClass(Bai3.class);
        job2.setReducerClass(Job2Reduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(intermediateOutput), TextInputFormat.class, GenderRatingMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
