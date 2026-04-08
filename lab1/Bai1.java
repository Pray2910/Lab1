package lab1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

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
            String movieName = parts[1];
            context.write(new Text(movieId), new Text("M::" + movieName));
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            String moviesId = parts[1];
            float rating = Float.parseFloat(parts[2]);
            context.write(new Text(moviesId), new Text("R::" + rating));
        }
    }

    public static class CustomReducer extends Reducer<Text, Text, Text, Text> {
        
        private String maxMovieName = "";
        private float maxAverageRating = 0.0f;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            String movieName = "";
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.startsWith("R::")) {
                    float rating = Float.parseFloat(val.substring(3));
                    sum += rating;
                    count++;
                }
                else {
                    movieName = val.substring(3);
                }
            }
            float average = sum / count;
            if (count >= 5 && average > maxAverageRating) {
                maxAverageRating = average;
                maxMovieName = movieName;
            }
            String result = String.format("AverageRating: %.2f (TotalRatings: %d)", average, count);
            context.write(new Text(movieName), new Text(result));
        }

        // chạy khi reduce xong
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovieName.isEmpty()) {
                context.write(
                    new Text(maxMovieName),
                    new Text(String.format(
                        "là phim có đánh giá trung bình cao nhất: %.2f",
                        maxAverageRating
                    ))
                );
            }
        }
    }
    
    // args: movies ratings_1 ratings_2 output
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Initialize job with name
        Job job = Job.getInstance(conf, "Movie Ratings");
        
        job.setJarByClass(Bai1.class);
        job.setReducerClass(CustomReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}  