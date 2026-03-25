package lab1;

import java.io.IOException;
import java.util.HashMap;
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

public class Bai2 {

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
            String genres = parts[2];
            context.write(new Text(movieId), new Text("M::" + genres));
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

        private Map<String, float[]> genreRatingSum = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genres = "";
            float ratingSum = 0.0f;
            int ratingCount = 0;

            for (Text value : values) {
                String val = value.toString().trim();
                if (val.startsWith("M::")) {
                    genres = val.substring(3);
                } else if (val.startsWith("R::")) {
                    float rating = Float.parseFloat(val.substring(3));
                    ratingSum += rating;
                    ratingCount++;
                }
            }

            if (!genres.isEmpty() && ratingCount > 0) {
                // Vì split dùng regex nên nếu viết "|" thì sẽ bị hiểu là or
                String[] genreList = genres.split("\\|");
                for (String genre : genreList) {
                    genre = genre.trim();
                    genreRatingSum.putIfAbsent(genre, new float[]{0.0f, 0.0f});
                    genreRatingSum.get(genre)[0] += ratingSum;
                    genreRatingSum.get(genre)[1] += ratingCount;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, float[]> entry : genreRatingSum.entrySet()) {
            String genre = entry.getKey();
            float totalAvg = entry.getValue()[0];
            float count = entry.getValue()[1];
            float avgRating = totalAvg / count;

            context.write(
                new Text(genre),
                new Text(String.format("Avg: %.2f, Count: %.0f", avgRating, count))
            );
        }
        }

    }

    // args: movies ratings_1 ratings_2 output
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Initialize job with name
        Job job = Job.getInstance(conf, "Genre Ratings");
        
        job.setJarByClass(Bai2.class);
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
